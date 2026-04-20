//! Binance bookTicker WebSocket client with auto-reconnect.
//!
//! Streams real-time best bid/offer updates from Binance.
//! Simpler than the depth client — no REST snapshot sync needed.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use crate::websocket::reconnect::{ReconnectConfig, ReconnectState};

use super::messages::{parse_book_ticker, BinanceBookTicker};

/// WebSocket endpoint for Binance Futures streams.
const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/ws";

/// Binance 24-hour connection limit (proactively reconnect before this).
const MAX_CONNECTION_DURATION: Duration = Duration::from_secs(23 * 60 * 60);

/// Events emitted by the bookTicker client.
#[derive(Debug, Clone)]
pub enum BookTickerEvent {
    /// New bookTicker update
    Update(BinanceBookTicker),

    /// Disconnected from WebSocket
    Disconnected(String),

    /// Error occurred
    Error(String),
}

/// Statistics for the bookTicker WebSocket connection.
#[derive(Debug, Default)]
pub struct BookTickerWsStats {
    /// Total messages received
    pub messages_received: AtomicU64,

    /// Total reconnection attempts
    pub reconnects: AtomicU64,

    /// Total bytes received
    pub bytes_received: AtomicU64,
}

impl BookTickerWsStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> BookTickerWsStatsSnapshot {
        BookTickerWsStatsSnapshot {
            messages_received: self.messages_received.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of bookTicker WebSocket stats.
#[derive(Debug, Clone)]
pub struct BookTickerWsStatsSnapshot {
    pub messages_received: u64,
    pub reconnects: u64,
    pub bytes_received: u64,
}

/// Configuration for the bookTicker client.
#[derive(Debug, Clone)]
pub struct BookTickerClientConfig {
    /// Symbol to subscribe (e.g., "btcusdt")
    pub symbol: String,

    /// WebSocket URL
    pub ws_url: String,

    /// Reconnection configuration
    pub reconnect: ReconnectConfig,
}

impl Default for BookTickerClientConfig {
    fn default() -> Self {
        Self {
            symbol: "btcusdt".to_string(),
            ws_url: BINANCE_FUTURES_WS_URL.to_string(),
            reconnect: ReconnectConfig::for_orderbook(),
        }
    }
}

impl BookTickerClientConfig {
    /// Create a config for BTCUSDT Futures.
    pub fn btcusdt() -> Self {
        Self::default()
    }

    /// Create config for a specific symbol.
    pub fn futures(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_lowercase(),
            ..Self::default()
        }
    }

    /// Build the WebSocket stream URL.
    pub fn ws_stream_url(&self) -> String {
        format!("{}/{}@bookTicker", self.ws_url, self.symbol)
    }
}

/// Binance bookTicker WebSocket client with auto-reconnect.
pub struct BinanceBookTickerClient {
    config: BookTickerClientConfig,
    running: AtomicBool,
    stats: BookTickerWsStats,
}

impl BinanceBookTickerClient {
    /// Create a new bookTicker client with default configuration.
    pub fn new(symbol: &str) -> Self {
        Self::with_config(BookTickerClientConfig::futures(symbol))
    }

    /// Create a new bookTicker client with custom configuration.
    pub fn with_config(config: BookTickerClientConfig) -> Self {
        Self {
            config,
            running: AtomicBool::new(false),
            stats: BookTickerWsStats::new(),
        }
    }

    /// Start the client and return a receiver for events.
    pub async fn run(self: Arc<Self>) -> mpsc::Receiver<BookTickerEvent> {
        let (tx, rx) = mpsc::channel(256);

        self.running.store(true, Ordering::Release);

        let client = self.clone();
        tokio::spawn(async move {
            client.connection_loop(tx).await;
        });

        rx
    }

    /// Stop the client gracefully.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Check if the client is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Get statistics.
    pub fn stats(&self) -> &BookTickerWsStats {
        &self.stats
    }

    /// Main connection loop with auto-reconnect.
    async fn connection_loop(&self, tx: mpsc::Sender<BookTickerEvent>) {
        let mut reconnect_state = ReconnectState::new(&self.config.reconnect);

        while self.running.load(Ordering::Acquire) {
            let connection_start = Instant::now();

            match self.connect_and_stream(&tx, connection_start).await {
                Ok(_) => {
                    reconnect_state.reset(&self.config.reconnect);
                }
                Err(e) => {
                    error!(error = %e, "BookTicker connection error");
                    let _ = tx.send(BookTickerEvent::Error(e.to_string())).await;
                }
            }

            if !self.running.load(Ordering::Acquire) {
                break;
            }

            // Apply reconnection backoff
            if let Some(delay) = reconnect_state.next_delay(&self.config.reconnect) {
                let _ = tx
                    .send(BookTickerEvent::Disconnected(format!(
                        "Reconnecting in {}s",
                        delay
                    )))
                    .await;
                self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
                sleep(Duration::from_secs(delay)).await;
            } else {
                error!("BookTicker max reconnection attempts exceeded");
                let _ = tx
                    .send(BookTickerEvent::Error(
                        "Max reconnection attempts exceeded".to_string(),
                    ))
                    .await;
                break;
            }
        }

        info!("BookTicker client stopped");
    }

    /// Connect to WebSocket and stream bookTicker updates.
    async fn connect_and_stream(
        &self,
        tx: &mpsc::Sender<BookTickerEvent>,
        connection_start: Instant,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ws_url = self.config.ws_stream_url();
        info!(url = %ws_url, "Connecting to Binance bookTicker WebSocket");

        let connect_timeout = Duration::from_secs(self.config.reconnect.connect_timeout_secs);
        let (ws_stream, _response) = timeout(connect_timeout, connect_async(&ws_url)).await??;

        let (mut write, mut read) = ws_stream.split();

        info!("BookTicker WebSocket connected");

        let stale_timeout = Duration::from_secs(self.config.reconnect.stale_timeout_secs);
        let mut last_message_time = Instant::now();

        // Main message loop
        while self.running.load(Ordering::Acquire) {
            // Check for 24-hour connection limit
            if connection_start.elapsed() > MAX_CONNECTION_DURATION {
                info!("BookTicker approaching 24h limit, reconnecting");
                break;
            }

            // Check for stale connection
            if last_message_time.elapsed() > stale_timeout {
                warn!(
                    elapsed_secs = last_message_time.elapsed().as_secs(),
                    "BookTicker connection stale, reconnecting"
                );
                break;
            }

            // Read next message with timeout
            let read_timeout = Duration::from_secs(5);
            let msg_result = timeout(read_timeout, read.next()).await;

            match msg_result {
                Ok(Some(Ok(msg))) => {
                    last_message_time = Instant::now();

                    // Track bytes
                    let bytes = match &msg {
                        Message::Text(t) => t.len() as u64,
                        Message::Binary(b) => b.len() as u64,
                        _ => 0,
                    };
                    self.stats.bytes_received.fetch_add(bytes, Ordering::Relaxed);

                    // Handle ping
                    if let Message::Ping(data) = &msg {
                        let _ = write.send(Message::Pong(data.clone())).await;
                        continue;
                    }

                    // Parse bookTicker
                    let ticker = match parse_book_ticker(&msg)? {
                        Some(t) => t,
                        None => continue,
                    };

                    self.stats.messages_received.fetch_add(1, Ordering::Relaxed);

                    let _ = tx.send(BookTickerEvent::Update(ticker)).await;
                }
                Ok(Some(Err(e))) => {
                    error!(error = %e, "BookTicker WebSocket error");
                    return Err(e.into());
                }
                Ok(None) => {
                    info!("BookTicker WebSocket stream closed");
                    break;
                }
                Err(_) => {
                    // Timeout — continue loop to check stale/running
                    continue;
                }
            }
        }

        // Graceful close
        let _ = write.send(Message::Close(None)).await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BookTickerClientConfig::default();
        assert_eq!(config.symbol, "btcusdt");
        assert_eq!(
            config.ws_stream_url(),
            "wss://fstream.binance.com/ws/btcusdt@bookTicker"
        );
    }

    #[test]
    fn test_futures_config() {
        let config = BookTickerClientConfig::futures("ethusdt");
        assert_eq!(
            config.ws_stream_url(),
            "wss://fstream.binance.com/ws/ethusdt@bookTicker"
        );
    }

    #[test]
    fn test_stats_default() {
        let stats = BookTickerWsStats::new();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.messages_received, 0);
        assert_eq!(snapshot.reconnects, 0);
        assert_eq!(snapshot.bytes_received, 0);
    }
}
