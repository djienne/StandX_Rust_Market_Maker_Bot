//! WebSocket client for order operations (low latency).
//!
//! This module provides a WebSocket client for placing and canceling orders
//! with minimal latency. Uses the StandX ws-api/v1 endpoint.
//!
//! Features automatic reconnection with exponential backoff (max 10 retries).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::HashMap;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{timeout, sleep, Instant};
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::auth::AuthManager;
use super::client::NewOrderRequest;
use crate::websocket::reconnect::{ReconnectConfig, ReconnectState};

/// Order WebSocket errors.
#[derive(Debug, thiserror::Error)]
pub enum OrderWsError {
    #[error("Not connected")]
    NotConnected,

    #[error("Not authenticated")]
    NotAuthenticated,

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Send error: {0}")]
    SendError(String),

    #[error("Auth error: {0}")]
    AuthError(#[from] super::auth::AuthError),

    #[error("Authentication timeout")]
    AuthTimeout,

    #[error("Authentication rejected: {0}")]
    AuthRejected(String),
}

/// Order events received from WebSocket.
#[derive(Debug, Clone)]
pub enum OrderEvent {
    /// Connected to WebSocket server.
    Connected,
    /// Authenticated on WebSocket.
    Authenticated,
    /// Order accepted by exchange.
    OrderAccepted { cl_ord_id: String, order_id: i64 },
    /// Exchange accepted an order submission request but omitted order identity.
    /// The order remains Pending until authenticated REST observation confirms it.
    OrderSubmissionAcknowledged,
    /// Order rejected by exchange.
    OrderRejected { cl_ord_id: String, reason: String },
    /// Order fill/update with correlation and status where supplied.
    OrderFilled {
        order_id: i64,
        cl_ord_id: Option<String>,
        status: Option<String>,
        fill_qty: String,
        fill_price: String,
    },
    /// Order canceled.
    OrderCanceled { order_id: i64, cl_ord_id: Option<String> },
    /// Exchange accepted a cancel request but omitted order identity.
    /// Local state remains Canceling until an identified update or REST cleanup.
    CancelSubmissionAcknowledged,
    /// Cancel request failed.
    CancelFailed { order_id: i64, reason: String },
    /// WebSocket disconnected (will attempt reconnect).
    Disconnected(String),
    /// Reconnecting after disconnect.
    Reconnecting { attempt: u32, delay_secs: u64 },
    /// Max reconnection attempts exceeded - fatal error.
    MaxRetriesExceeded,
    /// General error.
    Error(String),
    /// A response could not be correlated safely to a request.
    AmbiguousResponse {
        request_id: Option<String>,
        code: Option<i32>,
        reason: String,
    },
}

/// WebSocket message for order API.
#[derive(Debug, Serialize)]
struct WsOrderMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    header: Option<WsHeader>,
    /// Params as JSON string (not nested object) for order methods
    params: WsParams,
}

/// Params wrapper that serializes as a JSON string.
#[derive(Debug)]
struct WsParams(String);

impl Serialize for WsParams {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

#[derive(Debug, Serialize)]
struct WsHeader {
    #[serde(rename = "x-request-id")]
    request_id: String,
    #[serde(rename = "x-request-timestamp")]
    timestamp: String,
    #[serde(rename = "x-request-signature")]
    signature: String,
}

/// Response from WebSocket.
#[derive(Debug, Deserialize)]
struct WsResponse {
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    code: Option<i32>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    data: Option<serde_json::Value>,
    /// Result field used by StandX responses (alternative to data)
    #[serde(default)]
    result: Option<serde_json::Value>,
    /// Request ID to correlate responses
    #[serde(default, alias = "requestId", alias = "x-request-id")]
    request_id: Option<String>,
}

#[derive(Debug, Clone)]
enum PendingRequest {
    New { cl_ord_id: String },
    CancelByClientId { cl_ord_id: String },
    CancelByOrderId { order_id: i64 },
}

type WsWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsReader = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// WebSocket client for order operations with automatic reconnection.
pub struct OrderWsClient {
    /// WebSocket URL
    url: String,
    /// Auth manager for JWT and signing
    auth: Arc<Mutex<AuthManager>>,
    /// WebSocket write half (protected by mutex for Arc<Self> access)
    writer: Arc<Mutex<Option<WsWriter>>>,
    /// Whether connected
    connected: Arc<AtomicBool>,
    /// Whether authenticated on WebSocket
    ws_authenticated: Arc<AtomicBool>,
    /// Whether the client is running (for connection loop)
    running: Arc<AtomicBool>,
    /// Session ID for this connection
    session_id: String,
    /// Message counter for request IDs
    message_counter: AtomicU64,
    /// Reconnection configuration
    reconnect_config: ReconnectConfig,
    /// Pending cancel cl_ord_ids (to distinguish cancel confirmations from order acceptances)
    pending_cancels: Arc<Mutex<std::collections::HashSet<String>>>,
    /// Top-level WebSocket request IDs mapped to the operation they identify.
    pending_requests: Arc<Mutex<HashMap<String, PendingRequest>>>,
}

impl OrderWsClient {
    /// Create a new order WebSocket client with default config.
    pub fn new(auth: Arc<Mutex<AuthManager>>) -> Self {
        Self {
            url: "wss://perps.standx.com/ws-api/v1".to_string(),
            auth,
            writer: Arc::new(Mutex::new(None)),
            connected: Arc::new(AtomicBool::new(false)),
            ws_authenticated: Arc::new(AtomicBool::new(false)),
            running: Arc::new(AtomicBool::new(false)),
            session_id: format!("session_{}", chrono::Utc::now().timestamp_millis()),
            message_counter: AtomicU64::new(0),
            reconnect_config: ReconnectConfig::for_orders(), // Max 10 retries
            pending_cancels: Arc::new(Mutex::new(std::collections::HashSet::with_capacity(8))),
            pending_requests: Arc::new(Mutex::new(HashMap::with_capacity(8))),
        }
    }

    /// Create with custom URL and reconnection config.
    pub fn with_config(
        auth: Arc<Mutex<AuthManager>>,
        url: impl Into<String>,
        reconnect_config: ReconnectConfig,
    ) -> Self {
        Self {
            url: url.into(),
            auth,
            writer: Arc::new(Mutex::new(None)),
            connected: Arc::new(AtomicBool::new(false)),
            ws_authenticated: Arc::new(AtomicBool::new(false)),
            running: Arc::new(AtomicBool::new(false)),
            session_id: format!("session_{}", chrono::Utc::now().timestamp_millis()),
            message_counter: AtomicU64::new(0),
            reconnect_config,
            pending_cancels: Arc::new(Mutex::new(std::collections::HashSet::with_capacity(8))),
            pending_requests: Arc::new(Mutex::new(HashMap::with_capacity(8))),
        }
    }

    /// Create with custom URL (uses default reconnect config).
    pub fn with_url(auth: Arc<Mutex<AuthManager>>, url: impl Into<String>) -> Self {
        let mut client = Self::new(auth);
        client.url = url.into();
        client
    }

    /// Set custom reconnection configuration.
    pub fn with_reconnect_config(mut self, config: ReconnectConfig) -> Self {
        self.reconnect_config = config;
        self
    }

    /// Check if connected.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// Check if WebSocket is authenticated.
    pub fn is_ws_authenticated(&self) -> bool {
        self.ws_authenticated.load(Ordering::Acquire)
    }

    /// Check if the client is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Stop the client (will stop reconnection attempts).
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
        self.ws_authenticated.store(false, Ordering::Release);
    }

    /// Generate a unique request ID.
    fn next_request_id(&self) -> String {
        let count = self.message_counter.fetch_add(1, Ordering::Relaxed);
        format!("req_{}_{}", self.session_id, count)
    }

    /// Run the WebSocket client with automatic reconnection.
    ///
    /// Returns a receiver for order events. Spawns a background task
    /// that handles reconnection with exponential backoff.
    pub async fn run(self: Arc<Self>) -> mpsc::Receiver<OrderEvent> {
        let (tx, rx) = mpsc::channel(1000);
        let client = Arc::clone(&self);

        self.running.store(true, Ordering::Release);

        tokio::spawn(async move {
            client.connection_loop(tx).await;
        });

        rx
    }

    /// Main connection loop with exponential backoff reconnection.
    async fn connection_loop(&self, tx: mpsc::Sender<OrderEvent>) {
        let mut reconnect_state = ReconnectState::new(&self.reconnect_config);

        while self.running.load(Ordering::Acquire) {
            info!("Connecting to order WebSocket: {}", self.url);

            match self.connect_and_run(&tx).await {
                Ok(_) => {
                    info!("Order WebSocket connection closed gracefully");
                    reconnect_state.reset(&self.reconnect_config);
                }
                Err(e) => {
                    error!("Order WebSocket connection error: {}", e);
                    let _ = tx.send(OrderEvent::Error(e.to_string())).await;
                }
            }

            // Mark as disconnected and clear stale state
            self.connected.store(false, Ordering::Release);
            self.ws_authenticated.store(false, Ordering::Release);
            self.pending_cancels.lock().await.clear();
            self.pending_requests.lock().await.clear();

            if !self.running.load(Ordering::Acquire) {
                break;
            }

            // Send disconnected event
            let _ = tx.send(OrderEvent::Disconnected("Connection lost".to_string())).await;

            // Calculate next delay (returns None if max retries exceeded)
            match reconnect_state.next_delay(&self.reconnect_config) {
                Some(delay) => {
                    let attempt = reconnect_state.consecutive_failures();
                    warn!(
                        "Order WebSocket reconnecting in {}s (attempt {}/{})",
                        delay,
                        attempt,
                        self.reconnect_config.max_retries.unwrap_or(u32::MAX)
                    );
                    let _ = tx.send(OrderEvent::Reconnecting { attempt, delay_secs: delay }).await;
                    sleep(Duration::from_secs(delay)).await;
                }
                None => {
                    error!(
                        "Order WebSocket max retries ({}) exceeded, giving up",
                        self.reconnect_config.max_retries.unwrap_or(0)
                    );
                    let _ = tx.send(OrderEvent::MaxRetriesExceeded).await;
                    self.running.store(false, Ordering::Release);
                    break;
                }
            }
        }

        info!("Order WebSocket client stopped");
    }

    /// Connect, authenticate, and run the message loop.
    async fn connect_and_run(&self, tx: &mpsc::Sender<OrderEvent>) -> Result<(), OrderWsError> {
        // Connect with timeout
        let connect_timeout = Duration::from_secs(self.reconnect_config.connect_timeout_secs);
        let (ws_stream, _) = timeout(connect_timeout, connect_async(&self.url))
            .await
            .map_err(|_| OrderWsError::ConnectionError("Connection timeout".to_string()))?
            .map_err(|e| OrderWsError::ConnectionError(e.to_string()))?;

        info!("Order WebSocket connected");
        self.connected.store(true, Ordering::Release);

        let (write, mut read) = ws_stream.split();

        // Store writer for use by place_order/cancel_order
        {
            let mut writer_guard = self.writer.lock().await;
            *writer_guard = Some(write);
        }

        // Send connected event
        let _ = tx.send(OrderEvent::Connected).await;

        // Authenticate (pass read half to validate response)
        self.ws_authenticate(&mut read).await?;

        // Send authenticated event
        let _ = tx.send(OrderEvent::Authenticated).await;

        // Message receive loop
        let stale_timeout = Duration::from_secs(self.reconnect_config.stale_timeout_secs);
        let mut last_message = Instant::now();

        loop {
            if !self.running.load(Ordering::Acquire) {
                break;
            }

            // Check for stale connection
            if last_message.elapsed() > stale_timeout {
                warn!("Order WebSocket connection stale (no message for {:?}), reconnecting", stale_timeout);
                break;
            }

            // Read with timeout (use stale_timeout as read deadline)
            let read_timeout = stale_timeout;
            match timeout(read_timeout, read.next()).await {
                Ok(Some(Ok(msg))) => {
                    last_message = Instant::now();
                    match msg {
                        Message::Text(text) => {
                            match serde_json::from_str::<WsResponse>(&text) {
                                Ok(response) => {
                                    debug!(
                                        "Order WS response: method={:?}, code={:?}, request_id={:?}",
                                        response.method, response.code, response.request_id
                                    );
                                    Self::handle_response(
                                        response,
                                        tx,
                                        &self.pending_cancels,
                                        &self.pending_requests,
                                    )
                                    .await;
                                }
                                Err(error) => {
                                    let _ = tx
                                        .send(OrderEvent::AmbiguousResponse {
                                            request_id: None,
                                            code: None,
                                            reason: format!("malformed order WS response: {error}"),
                                        })
                                        .await;
                                }
                            }
                        }
                        Message::Ping(_) => {
                            debug!("Received ping (pong handled automatically)");
                        }
                        Message::Pong(_) => {
                            debug!("Received pong");
                        }
                        Message::Close(frame) => {
                            info!("WebSocket close frame received: {:?}", frame);
                            break;
                        }
                        _ => {}
                    }
                }
                Ok(Some(Err(e))) => {
                    error!("WebSocket read error: {}", e);
                    return Err(OrderWsError::ConnectionError(e.to_string()));
                }
                Ok(None) => {
                    info!("WebSocket stream ended");
                    break;
                }
                Err(_) => {
                    // Timeout, continue loop (stale check handles actual staleness)
                    continue;
                }
            }
        }

        // Clear writer on disconnect
        {
            let mut writer_guard = self.writer.lock().await;
            *writer_guard = None;
        }

        self.connected.store(false, Ordering::Release);
        self.ws_authenticated.store(false, Ordering::Release);

        Ok(())
    }

    /// Authenticate on the WebSocket connection.
    ///
    /// Waits for the actual auth:login response and validates the code.
    /// Returns error if auth is rejected or times out.
    async fn ws_authenticate(&self, read: &mut WsReader) -> Result<(), OrderWsError> {
        // Get JWT token from auth manager
        let token = {
            let mut auth = self.auth.lock().await;
            // Ensure we have a valid token
            if !auth.is_authenticated() {
                auth.authenticate().await?;
            }
            auth.jwt().map(|s| s.to_string())
        };

        let token = token.ok_or(OrderWsError::NotAuthenticated)?;

        // Send auth:login message - params must be JSON string
        let auth_params = serde_json::to_string(&json!({ "token": token }))
            .map_err(|e| OrderWsError::SendError(e.to_string()))?;
        let msg = WsOrderMessage {
            session_id: Some(self.session_id.clone()),
            request_id: Some(self.next_request_id()),
            method: "auth:login".to_string(),
            header: None,
            params: WsParams(auth_params),
        };

        let request_id = msg.request_id.clone();
        self.send_message_internal(&msg).await?;

        // Wait for auth response with timeout (5 seconds)
        // Response format: { "code": 0, "message": "success", "request_id": "..." }
        let auth_timeout = Duration::from_secs(5);
        let start = Instant::now();

        while start.elapsed() < auth_timeout {
            let read_timeout = Duration::from_millis(500);
            match timeout(read_timeout, read.next()).await {
                Ok(Some(Ok(Message::Text(text)))) => {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                        // Check if this is an auth response (has code field and matches our request_id)
                        let resp_request_id = json.get("request_id").and_then(|r| r.as_str());
                        let has_code = json.get("code").is_some();
                        debug!(
                            "Order WS auth response: code={:?}, request_id={:?}",
                            json.get("code").and_then(|code| code.as_i64()),
                            resp_request_id
                        );

                        // Match by request_id if present, or accept any response with code during auth
                        if has_code && (resp_request_id == request_id.as_deref() || resp_request_id.is_none()) {
                            let code = json.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
                            if code == 0 {
                                self.ws_authenticated.store(true, Ordering::Release);
                                info!("Order WebSocket authenticated successfully");
                                return Ok(());
                            } else {
                                let msg = json.get("message")
                                    .or_else(|| json.get("msg"))
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("Unknown auth error")
                                    .to_string();
                                error!("Order WebSocket auth rejected: code={}, msg={}", code, msg);
                                return Err(OrderWsError::AuthRejected(msg));
                            }
                        }
                        // Not an auth response, continue waiting
                    }
                }
                Ok(Some(Ok(Message::Ping(_)))) | Ok(Some(Ok(Message::Pong(_)))) => {
                    // Ignore ping/pong during auth
                    continue;
                }
                Ok(Some(Ok(Message::Close(frame)))) => {
                    return Err(OrderWsError::ConnectionError(
                        format!("Connection closed during auth: {:?}", frame)
                    ));
                }
                Ok(Some(Err(e))) => {
                    return Err(OrderWsError::ConnectionError(e.to_string()));
                }
                Ok(None) => {
                    return Err(OrderWsError::ConnectionError("Stream ended during auth".to_string()));
                }
                Err(_) => {
                    // Read timeout, continue loop (overall timeout handles actual timeout)
                    continue;
                }
                _ => continue,
            }
        }

        error!("Order WebSocket auth timeout after {}s", auth_timeout.as_secs());
        Err(OrderWsError::AuthTimeout)
    }

    /// Internal message send (locks writer).
    async fn send_message_internal(&self, msg: &WsOrderMessage) -> Result<(), OrderWsError> {
        let mut writer_guard = self.writer.lock().await;
        let writer = writer_guard.as_mut()
            .ok_or(OrderWsError::NotConnected)?;

        let json = serde_json::to_string(msg)
            .map_err(|e| OrderWsError::SendError(e.to_string()))?;

        debug!("{}", Self::safe_log_summary(msg));

        writer.send(Message::Text(json)).await
            .map_err(|e| OrderWsError::SendError(e.to_string()))?;

        Ok(())
    }

    fn safe_log_summary(msg: &WsOrderMessage) -> String {
        format!(
            "Sending order WS message: method={}, request_id={:?}",
            msg.method, msg.request_id
        )
    }

    /// Place a new order via WebSocket.
    ///
    /// Returns the client order ID.
    pub async fn place_order(&self, mut request: NewOrderRequest) -> Result<String, OrderWsError> {
        if !self.is_connected() {
            return Err(OrderWsError::NotConnected);
        }
        if !self.is_ws_authenticated() {
            return Err(OrderWsError::NotAuthenticated);
        }

        // Generate client order ID if not provided
        let cl_ord_id = request.cl_ord_id.take()
            .unwrap_or_else(|| format!("ord_{}", chrono::Utc::now().timestamp_millis()));

        // Set cl_ord_id back
        request.cl_ord_id = Some(cl_ord_id.clone());

        // Sign the request
        let params_json = serde_json::to_string(&request)
            .map_err(|e| OrderWsError::SendError(e.to_string()))?;

        let (signing_request_id, timestamp_ms, signature) = {
            let auth = self.auth.lock().await;
            auth.sign_request(&params_json)?
        };
        let response_request_id = self.next_request_id();

        // Build WebSocket message
        let msg = WsOrderMessage {
            session_id: Some(self.session_id.clone()),
            request_id: Some(response_request_id.clone()),
            method: "order:new".to_string(),
            header: Some(WsHeader {
                request_id: signing_request_id,
                timestamp: timestamp_ms.to_string(),
                signature,
            }),
            params: WsParams(params_json),
        };

        self.pending_requests.lock().await.insert(
            response_request_id.clone(),
            PendingRequest::New {
                cl_ord_id: cl_ord_id.clone(),
            },
        );
        if let Err(error) = self.send_message_internal(&msg).await {
            self.pending_requests
                .lock()
                .await
                .remove(&response_request_id);
            return Err(error);
        }

        Ok(cl_ord_id)
    }

    /// Cancel an order by order ID.
    pub async fn cancel_order(&self, order_id: i64) -> Result<(), OrderWsError> {
        self.send_cancel(
            json!({ "order_id": order_id }),
            PendingRequest::CancelByOrderId { order_id },
        )
        .await
    }

    /// Cancel an order by client order ID.
    pub async fn cancel_order_by_client_id(&self, cl_ord_id: &str) -> Result<(), OrderWsError> {
        // Track this cl_ord_id as pending cancel to distinguish from order acceptance
        {
            let mut pending = self.pending_cancels.lock().await;
            pending.insert(cl_ord_id.to_string());
        }
        let result = self
            .send_cancel(
                json!({ "cl_ord_id": cl_ord_id }),
                PendingRequest::CancelByClientId {
                    cl_ord_id: cl_ord_id.to_string(),
                },
            )
            .await;
        if result.is_err() {
            self.pending_cancels.lock().await.remove(cl_ord_id);
        }
        result
    }

    /// Get reference to pending cancels set (for response handling).
    pub fn pending_cancels(&self) -> Arc<Mutex<std::collections::HashSet<String>>> {
        Arc::clone(&self.pending_cancels)
    }

    /// Drop request-correlation state after REST has proved that the account
    /// has no open orders. Any later response is stale and will fail closed.
    pub async fn clear_pending_tracking(&self) {
        self.pending_cancels.lock().await.clear();
        self.pending_requests.lock().await.clear();
    }

    /// Remove new-order requests that authenticated REST has identified.
    /// Uses try_lock so a cold-path snapshot can never stall the main event loop.
    pub fn try_clear_rest_confirmed_requests(&self, cl_ord_ids: &[&str]) {
        let Ok(mut pending) = self.pending_requests.try_lock() else {
            return;
        };
        pending.retain(|_, request| match request {
            PendingRequest::New { cl_ord_id } => !cl_ord_ids.contains(&cl_ord_id.as_str()),
            PendingRequest::CancelByClientId { .. } | PendingRequest::CancelByOrderId { .. } => {
                true
            }
        });
    }

    /// Internal helper to send a cancel request.
    async fn send_cancel(
        &self,
        params: serde_json::Value,
        pending_request: PendingRequest,
    ) -> Result<(), OrderWsError> {
        if !self.is_connected() {
            return Err(OrderWsError::NotConnected);
        }
        if !self.is_ws_authenticated() {
            return Err(OrderWsError::NotAuthenticated);
        }

        let params_json = serde_json::to_string(&params)
            .map_err(|e| OrderWsError::SendError(e.to_string()))?;

        let (signing_request_id, timestamp_ms, signature) = {
            let auth = self.auth.lock().await;
            auth.sign_request(&params_json)?
        };
        let response_request_id = self.next_request_id();

        let msg = WsOrderMessage {
            session_id: Some(self.session_id.clone()),
            request_id: Some(response_request_id.clone()),
            method: "order:cancel".to_string(),
            header: Some(WsHeader {
                request_id: signing_request_id,
                timestamp: timestamp_ms.to_string(),
                signature,
            }),
            params: WsParams(params_json),
        };

        self.pending_requests
            .lock()
            .await
            .insert(response_request_id.clone(), pending_request);
        if let Err(error) = self.send_message_internal(&msg).await {
            self.pending_requests
                .lock()
                .await
                .remove(&response_request_id);
            return Err(error);
        }

        Ok(())
    }

    /// Disconnect the WebSocket gracefully (permanent shutdown).
    pub async fn disconnect(&self) {
        self.running.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
        self.ws_authenticated.store(false, Ordering::Release);

        let mut writer_guard = self.writer.lock().await;
        if let Some(mut writer) = writer_guard.take() {
            let _ = writer.close().await;
        }

        info!("Order WebSocket disconnected");
    }

    /// Force a reconnection by closing the current connection.
    ///
    /// Unlike disconnect(), this keeps the client running so the
    /// connection loop will automatically reconnect and re-authenticate
    /// with a fresh JWT token. Used for proactive token refresh.
    pub async fn force_reconnect(&self) {
        if !self.is_connected() {
            return;
        }
        info!("Order WebSocket forcing reconnect for token refresh");
        self.connected.store(false, Ordering::Release);
        self.ws_authenticated.store(false, Ordering::Release);

        let mut writer_guard = self.writer.lock().await;
        if let Some(mut writer) = writer_guard.take() {
            let _ = writer.close().await;
        }
    }

    // ========== Field Extraction Helpers ==========
    // These handle multiple field name variants used by StandX API.

    /// Extract client order ID from JSON data.
    /// Handles: cl_ord_id, clOrdId, clientOrderId, client_order_id
    #[inline]
    fn extract_cl_ord_id(data: &serde_json::Value) -> Option<String> {
        data.get("cl_ord_id")
            .or_else(|| data.get("clOrdId"))
            .or_else(|| data.get("clientOrderId"))
            .or_else(|| data.get("client_order_id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// Extract exchange order ID from JSON data.
    /// Handles: id, order_id
    #[inline]
    fn extract_order_id(data: &serde_json::Value) -> Option<i64> {
        data.get("id")
            .or_else(|| data.get("order_id"))
            .and_then(|v| v.as_i64())
    }

    /// Extract fill quantity from JSON data.
    /// Handles: fill_qty, fillQty
    #[inline]
    fn extract_fill_qty(data: &serde_json::Value) -> String {
        data.get("fill_qty")
            .or_else(|| data.get("fillQty"))
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string()
    }

    /// Extract fill price from JSON data.
    /// Handles: fill_price, fillPrice
    #[inline]
    fn extract_fill_price(data: &serde_json::Value) -> String {
        data.get("fill_price")
            .or_else(|| data.get("fillPrice"))
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string()
    }

    #[inline]
    fn extract_status(data: &serde_json::Value) -> Option<String> {
        data.get("status")
            .or_else(|| data.get("order_status"))
            .or_else(|| data.get("orderStatus"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_ascii_lowercase())
    }

    // ========== Response Handlers ==========

    /// Handle a response from the server.
    #[inline]
    async fn handle_response(
        response: WsResponse,
        event_tx: &mpsc::Sender<OrderEvent>,
        pending_cancels: &Arc<Mutex<std::collections::HashSet<String>>>,
        pending_requests: &Arc<Mutex<HashMap<String, PendingRequest>>>,
    ) {
        let method = response.method.as_deref().unwrap_or("");
        let code = response.code.unwrap_or(0);
        let request_id = response.request_id.clone();

        if let Some(id) = request_id.as_deref() {
            let pending_request = pending_requests.lock().await.remove(id);
            if let Some(pending_request) = pending_request {
                Self::handle_correlated_response(
                    id,
                    pending_request,
                    response.code,
                    response.message.as_deref(),
                    event_tx,
                    pending_cancels,
                )
                .await;
                return;
            }
        }

        // Use result field if data is not present (StandX API uses "result")
        let data = response.data.or(response.result);

        // Handle responses with explicit method field
        match method {
            "order:new" => {
                debug!("Routing: order:new response (code={})", code);
                if response.code.is_none() {
                    let _ = event_tx
                        .send(OrderEvent::AmbiguousResponse {
                            request_id,
                            code: None,
                            reason: "order:new response omitted its result code".to_string(),
                        })
                        .await;
                    return;
                }
                if let Some(data) = data {
                    Self::handle_order_response(data, code, &response.message, event_tx).await;
                } else if code == 0 {
                    let _ = event_tx.send(OrderEvent::OrderSubmissionAcknowledged).await;
                } else {
                    let _ = event_tx
                        .send(OrderEvent::AmbiguousResponse {
                            request_id,
                            code: response.code,
                            reason: "order:new response contained no correlatable order data"
                                .to_string(),
                        })
                        .await;
                }
                return;
            }
            "order:cancel" => {
                debug!("Routing: order:cancel response (code={})", code);
                if response.code.is_none() {
                    let _ = event_tx
                        .send(OrderEvent::AmbiguousResponse {
                            request_id,
                            code: None,
                            reason: "order:cancel response omitted its result code".to_string(),
                        })
                        .await;
                    return;
                }
                if let Some(data) = data {
                    // Remove from pending cancels
                    if let Some(cl_ord_id) = Self::extract_cl_ord_id(&data) {
                        let mut pending = pending_cancels.lock().await;
                        pending.remove(&cl_ord_id);
                    }
                    Self::handle_cancel_response(data, code, &response.message, event_tx).await;
                } else if code == 0 {
                    let _ = event_tx.send(OrderEvent::CancelSubmissionAcknowledged).await;
                } else {
                    let _ = event_tx
                        .send(OrderEvent::AmbiguousResponse {
                            request_id,
                            code: response.code,
                            reason: "order:cancel response contained no correlatable order data"
                                .to_string(),
                        })
                        .await;
                }
                return;
            }
            "order:fill" | "order:filled" => {
                debug!("Routing: order:fill response");
                if let Some(data) = data {
                    let order_id = Self::extract_order_id(&data).unwrap_or(0);
                    let cl_ord_id = Self::extract_cl_ord_id(&data);
                    let status = Self::extract_status(&data).or_else(|| {
                        (method == "order:filled").then(|| "filled".to_string())
                    });
                    let fill_qty = Self::extract_fill_qty(&data);
                    let fill_price = Self::extract_fill_price(&data);
                    let _ = event_tx.send(OrderEvent::OrderFilled {
                        order_id,
                        cl_ord_id,
                        status,
                        fill_qty,
                        fill_price,
                    }).await;
                } else {
                    let _ = event_tx
                        .send(OrderEvent::AmbiguousResponse {
                            request_id,
                            code: response.code,
                            reason: "fill response contained no correlatable order data".to_string(),
                        })
                        .await;
                }
                return;
            }
            _ => {}
        }

        // Some deployed response variants omit or fail to echo the top-level
        // request ID. A code-zero response can still be classified when every
        // outstanding request is the same operation type, but it cannot promote
        // or clear any individual order without REST confirmation.
        if data.is_none() && response.code == Some(0) {
            let pending = pending_requests.lock().await;
            let has_new = pending
                .values()
                .any(|request| matches!(request, PendingRequest::New { .. }));
            let has_cancel = pending.values().any(|request| {
                matches!(
                    request,
                    PendingRequest::CancelByClientId { .. }
                        | PendingRequest::CancelByOrderId { .. }
                )
            });
            drop(pending);

            if has_new && !has_cancel {
                let _ = event_tx.send(OrderEvent::OrderSubmissionAcknowledged).await;
                return;
            }
            if has_cancel && !has_new {
                let _ = event_tx.send(OrderEvent::CancelSubmissionAcknowledged).await;
                return;
            }
        }

        // Handle responses WITHOUT method field (StandX API style)
        if let Some(data) = data {
            if response.code.is_none() {
                let _ = event_tx
                    .send(OrderEvent::AmbiguousResponse {
                        request_id,
                        code: None,
                        reason: "order response omitted its result code".to_string(),
                    })
                    .await;
                return;
            }
            let cl_ord_id = Self::extract_cl_ord_id(&data);

            // Check if this cl_ord_id is in our pending cancels list
            let is_pending_cancel = if let Some(ref id) = cl_ord_id {
                let pending = pending_cancels.lock().await;
                pending.contains(id)
            } else {
                false
            };

            if is_pending_cancel {
                // This is a cancel confirmation - remove from pending and fire event
                if let Some(ref id) = cl_ord_id {
                    let mut pending = pending_cancels.lock().await;
                    pending.remove(id);
                }
                debug!("Routing: cancel confirmation from pending_cancels (code={})", code);
                Self::handle_cancel_response(data, code, &response.message, event_tx).await;
                return;
            }

            // Not a pending cancel - must be an order acceptance
            if cl_ord_id.is_some() {
                debug!("Routing: order acceptance (not in pending_cancels) (code={})", code);
                Self::handle_order_response(data, code, &response.message, event_tx).await;
                return;
            }

            // Fallback: has order_id without cl_ord_id → cancel response
            let order_id = Self::extract_order_id(&data);
            if order_id.is_some() {
                debug!("Routing: inferred cancel response from order_id only (code={})", code);
                Self::handle_cancel_response(data, code, &response.message, event_tx).await;
                return;
            }
        }

        let reason = response.message.unwrap_or_else(|| {
            "response contained no method or correlatable order data".to_string()
        });
        let _ = event_tx
            .send(OrderEvent::AmbiguousResponse {
                request_id,
                code: response.code,
                reason,
            })
            .await;
    }

    /// Handle the documented StandX response shape, which carries only a
    /// top-level request ID, result code, and message.
    async fn handle_correlated_response(
        request_id: &str,
        pending_request: PendingRequest,
        code: Option<i32>,
        message: Option<&str>,
        event_tx: &mpsc::Sender<OrderEvent>,
        pending_cancels: &Arc<Mutex<std::collections::HashSet<String>>>,
    ) {
        let Some(code) = code else {
            let _ = event_tx
                .send(OrderEvent::AmbiguousResponse {
                    request_id: Some(request_id.to_string()),
                    code: None,
                    reason: "correlated order response omitted its result code".to_string(),
                })
                .await;
            return;
        };
        let reason = message.unwrap_or("exchange rejected request").to_string();

        match pending_request {
            PendingRequest::New { cl_ord_id } => {
                if code == 0 {
                    // StandX's response stream does not include an exchange order
                    // ID. Client ID correlation is sufficient because cancels use
                    // that same ID; REST will bind the numeric ID on its next poll.
                    let _ = event_tx
                        .send(OrderEvent::OrderAccepted {
                            cl_ord_id,
                            order_id: 0,
                        })
                        .await;
                } else {
                    let _ = event_tx
                        .send(OrderEvent::OrderRejected { cl_ord_id, reason })
                        .await;
                }
            }
            PendingRequest::CancelByClientId { cl_ord_id } => {
                pending_cancels.lock().await.remove(&cl_ord_id);
                if code == 0 {
                    let _ = event_tx
                        .send(OrderEvent::OrderCanceled {
                            order_id: 0,
                            cl_ord_id: Some(cl_ord_id),
                        })
                        .await;
                } else {
                    let _ = event_tx
                        .send(OrderEvent::CancelFailed {
                            order_id: 0,
                            reason,
                        })
                        .await;
                }
            }
            PendingRequest::CancelByOrderId { order_id } => {
                if code == 0 {
                    let _ = event_tx
                        .send(OrderEvent::OrderCanceled {
                            order_id,
                            cl_ord_id: None,
                        })
                        .await;
                } else {
                    let _ = event_tx
                        .send(OrderEvent::CancelFailed { order_id, reason })
                        .await;
                }
            }
        }
    }

    /// Handle order new/accepted response.
    #[inline]
    async fn handle_order_response(
        data: serde_json::Value,
        code: i32,
        message: &Option<String>,
        event_tx: &mpsc::Sender<OrderEvent>,
    ) {
        let cl_ord_id = Self::extract_cl_ord_id(&data).unwrap_or_default();
        let order_id = Self::extract_order_id(&data).unwrap_or(0);

        if code == 0 {
            if cl_ord_id.is_empty() || order_id == 0 {
                let _ = event_tx
                    .send(OrderEvent::OrderSubmissionAcknowledged)
                    .await;
                return;
            }
            info!("Order accepted: cl_ord_id={}, order_id={}", cl_ord_id, order_id);
            let _ = event_tx.send(OrderEvent::OrderAccepted { cl_ord_id, order_id }).await;
        } else {
            let reason = message.clone().unwrap_or_default();
            if cl_ord_id.is_empty() {
                let _ = event_tx
                    .send(OrderEvent::AmbiguousResponse {
                        request_id: None,
                        code: Some(code),
                        reason,
                    })
                    .await;
                return;
            }
            warn!("Order rejected: cl_ord_id={}, reason={}", cl_ord_id, reason);
            let _ = event_tx.send(OrderEvent::OrderRejected { cl_ord_id, reason }).await;
        }
    }

    /// Handle order cancel response.
    #[inline]
    async fn handle_cancel_response(
        data: serde_json::Value,
        code: i32,
        message: &Option<String>,
        event_tx: &mpsc::Sender<OrderEvent>,
    ) {
        let order_id = Self::extract_order_id(&data).unwrap_or(0);
        let cl_ord_id = Self::extract_cl_ord_id(&data);

        if code == 0 {
            if order_id == 0 && cl_ord_id.is_none() {
                let _ = event_tx
                    .send(OrderEvent::CancelSubmissionAcknowledged)
                    .await;
                return;
            }
            let _ = event_tx.send(OrderEvent::OrderCanceled { order_id, cl_ord_id }).await;
        } else {
            let reason = message.clone().unwrap_or_else(|| "Cancel failed".to_string());
            if order_id == 0 && cl_ord_id.is_none() {
                let _ = event_tx
                    .send(OrderEvent::AmbiguousResponse {
                        request_id: None,
                        code: Some(code),
                        reason,
                    })
                    .await;
                return;
            }
            warn!("Cancel failed for order {}: {}", order_id, reason);
            let _ = event_tx.send(OrderEvent::CancelFailed { order_id, reason }).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_generation() {
        let auth = Arc::new(Mutex::new(AuthManager::new()));
        let client = OrderWsClient::new(auth);

        let id1 = client.next_request_id();
        let id2 = client.next_request_id();

        assert_ne!(id1, id2);
        assert!(id1.starts_with("req_session_"));
    }

    #[test]
    fn test_reconnect_config() {
        let auth = Arc::new(Mutex::new(AuthManager::new()));
        let client = OrderWsClient::new(auth);

        // Default config should have max 10 retries
        assert_eq!(client.reconnect_config.max_retries, Some(10));
    }

    #[tokio::test]
    async fn data_less_order_response_fails_closed() {
        let response: WsResponse = serde_json::from_value(json!({
            "code": 0,
            "message": "success",
            "request_id": "constant-signing-id"
        }))
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let pending_requests = Arc::new(Mutex::new(HashMap::new()));

        OrderWsClient::handle_response(response, &tx, &pending, &pending_requests).await;

        assert!(matches!(
            rx.recv().await,
            Some(OrderEvent::AmbiguousResponse {
                request_id: Some(_),
                code: Some(0),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn documented_success_response_correlates_by_request_id() {
        let response: WsResponse = serde_json::from_value(json!({
            "code": 0,
            "message": "success",
            "request_id": "unique-request-id"
        }))
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let pending_requests = Arc::new(Mutex::new(HashMap::from([(
            "unique-request-id".to_string(),
            PendingRequest::New {
                cl_ord_id: "mm_TEST-USD_1_0_1".to_string(),
            },
        )])));

        OrderWsClient::handle_response(response, &tx, &pending, &pending_requests).await;

        assert!(matches!(
            rx.recv().await,
            Some(OrderEvent::OrderAccepted {
                ref cl_ord_id,
                order_id: 0
            }) if cl_ord_id == "mm_TEST-USD_1_0_1"
        ));
        assert!(pending_requests.lock().await.is_empty());
    }

    #[tokio::test]
    async fn documented_rejection_response_preserves_client_id() {
        let response: WsResponse = serde_json::from_value(json!({
            "code": 400,
            "message": "alo order rejected",
            "requestId": "unique-request-id"
        }))
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let pending_requests = Arc::new(Mutex::new(HashMap::from([(
            "unique-request-id".to_string(),
            PendingRequest::New {
                cl_ord_id: "mm_TEST-USD_1_0_1".to_string(),
            },
        )])));

        OrderWsClient::handle_response(response, &tx, &pending, &pending_requests).await;

        assert!(matches!(
            rx.recv().await,
            Some(OrderEvent::OrderRejected {
                ref cl_ord_id,
                ref reason
            }) if cl_ord_id == "mm_TEST-USD_1_0_1" && reason == "alo order rejected"
        ));
    }

    #[tokio::test]
    async fn un_echoed_success_with_only_new_requests_waits_for_rest() {
        let response: WsResponse = serde_json::from_value(json!({
            "code": 0,
            "message": "success",
            "request_id": "unexpected-signing-id"
        }))
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let pending_requests = Arc::new(Mutex::new(HashMap::from([(
            "unique-request-id".to_string(),
            PendingRequest::New {
                cl_ord_id: "mm_TEST-USD_1_0_1".to_string(),
            },
        )])));

        OrderWsClient::handle_response(response, &tx, &pending, &pending_requests).await;

        assert!(matches!(
            rx.recv().await,
            Some(OrderEvent::OrderSubmissionAcknowledged)
        ));
        assert_eq!(pending_requests.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn data_rich_order_response_preserves_correlation() {
        let response: WsResponse = serde_json::from_value(json!({
            "method": "order:new",
            "code": 0,
            "data": {"cl_ord_id": "mm_TEST-USD_1_0_1", "order_id": 42}
        }))
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let pending_requests = Arc::new(Mutex::new(HashMap::new()));

        OrderWsClient::handle_response(response, &tx, &pending, &pending_requests).await;

        assert!(matches!(
            rx.recv().await,
            Some(OrderEvent::OrderAccepted { order_id: 42, .. })
        ));
    }

    #[tokio::test]
    async fn identified_success_without_order_ids_waits_for_rest_confirmation() {
        let response: WsResponse = serde_json::from_value(json!({
            "method": "order:new",
            "code": 0,
            "message": "success",
            "data": {}
        }))
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let pending_requests = Arc::new(Mutex::new(HashMap::new()));

        OrderWsClient::handle_response(response, &tx, &pending, &pending_requests).await;

        assert!(matches!(
            rx.recv().await,
            Some(OrderEvent::OrderSubmissionAcknowledged)
        ));
    }

    #[tokio::test]
    async fn identified_success_without_cancel_ids_waits_for_rest_cleanup() {
        let response: WsResponse = serde_json::from_value(json!({
            "method": "order:cancel",
            "code": 0,
            "message": "success",
            "data": {}
        }))
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let pending_requests = Arc::new(Mutex::new(HashMap::new()));

        OrderWsClient::handle_response(response, &tx, &pending, &pending_requests).await;

        assert!(matches!(
            rx.recv().await,
            Some(OrderEvent::CancelSubmissionAcknowledged)
        ));
    }

    #[tokio::test]
    async fn identified_response_without_result_code_fails_closed() {
        let response: WsResponse = serde_json::from_value(json!({
            "method": "order:new",
            "message": "success",
            "data": {}
        }))
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let pending_requests = Arc::new(Mutex::new(HashMap::new()));

        OrderWsClient::handle_response(response, &tx, &pending, &pending_requests).await;

        assert!(matches!(
            rx.recv().await,
            Some(OrderEvent::AmbiguousResponse { code: None, .. })
        ));
    }

    #[test]
    fn outbound_log_summary_redacts_credentials_and_params() {
        let message = WsOrderMessage {
            session_id: Some("session".to_string()),
            request_id: Some("request".to_string()),
            method: "auth:login".to_string(),
            header: Some(WsHeader {
                request_id: "header-id".to_string(),
                timestamp: "123".to_string(),
                signature: "private-signature".to_string(),
            }),
            params: WsParams("{\"token\":\"secret-jwt\"}".to_string()),
        };

        let summary = OrderWsClient::safe_log_summary(&message);
        assert!(summary.contains("auth:login"));
        assert!(!summary.contains("secret-jwt"));
        assert!(!summary.contains("private-signature"));
        assert!(!summary.contains("header-id"));
    }
}
