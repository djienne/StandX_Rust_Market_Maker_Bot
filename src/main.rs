//! StandX Market Maker - Main Entry Point
//!
//! High-performance market making system for the StandX perpetual futures exchange.
//! Features OBI strategy, position management, and low-latency order execution.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::signal;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

use standx_orderbook::{
    Config, OrderbookStore, WsClientBuilder, WsEvent, StandXMessage,
    ObiStrategy, QuoteFormatter, init_logging,
    AuthManager, SharedPosition, PositionPoller, PositionPollerConfig, PositionPollerHandle,
    QuoteOrderManager, OrderManagerConfig, OrderDecision, Side,
    WalletTracker, WalletTrackerConfig, WalletTrackerHandle,
    OrderbookSanityChecker, SanityCheckerConfig, SanityCheckerHandle,
};
use standx_orderbook::trading::TradingStats;
use standx_orderbook::trading::{OrderWsClient, OrderEvent, NewOrderRequest};

/// Statistics tracker for monitoring throughput and rates.
/// Extracted from App to reduce struct bloat and separate concerns.
struct StatsTracker {
    /// Message counter for logging
    message_count: u64,
    /// Quote counter
    quote_count: u64,
    /// Order decision counter
    order_decision_count: u64,
    /// Last stats log time
    last_stats_time: std::time::Instant,
    /// Shared trading stats for volume/price tracking (used by WalletTracker)
    trading_stats: Arc<TradingStats>,
}

impl StatsTracker {
    fn new() -> Self {
        Self {
            message_count: 0,
            quote_count: 0,
            order_decision_count: 0,
            last_stats_time: std::time::Instant::now(),
            trading_stats: Arc::new(TradingStats::default()),
        }
    }

    /// Get trading stats for wallet tracker.
    fn trading_stats(&self) -> Arc<TradingStats> {
        Arc::clone(&self.trading_stats)
    }

    /// Log statistics if interval has elapsed.
    fn log_if_needed(
        &mut self,
        config: &Config,
        store: &OrderbookStore,
        strategies: &HashMap<String, ObiStrategy>,
        order_managers: &HashMap<String, QuoteOrderManager>,
    ) {
        // 0 means disabled (as documented)
        if config.stats_interval_secs == 0 {
            return;
        }
        let elapsed = self.last_stats_time.elapsed();
        if elapsed < Duration::from_secs(config.stats_interval_secs) {
            return;
        }

        let stats = store.stats();
        for stat in &stats {
            // Get strategy status for this symbol
            let strategy_status = strategies.get(&stat.symbol)
                .map(|s| if s.is_warmed_up() {
                    format!("vol={:.4} alpha={:.3}", s.volatility(), s.alpha())
                } else {
                    "warming up".to_string()
                })
                .unwrap_or_default();

            info!(
                "[{}] updates={} history={}/{} bid={:.2} ask={:.2} spread={:.4} {}",
                stat.symbol,
                stat.update_count,
                stat.history_count,
                config.history_buffer_size,
                stat.best_bid.unwrap_or(0.0),
                stat.best_ask.unwrap_or(0.0),
                stat.spread.unwrap_or(0.0),
                strategy_status,
            );
        }

        let rate = self.message_count as f64 / elapsed.as_secs_f64();
        let quote_rate = self.quote_count as f64 / elapsed.as_secs_f64();
        let order_rate = self.order_decision_count as f64 / elapsed.as_secs_f64();

        if config.order.enabled {
            info!(
                "Stats: {} messages ({:.1}/sec), {} quotes ({:.1}/sec), {} orders ({:.1}/sec)",
                self.message_count,
                rate,
                self.quote_count,
                quote_rate,
                self.order_decision_count,
                order_rate,
            );

            // Log order manager stats
            for (symbol, manager) in order_managers {
                let stats = manager.stats();
                if stats.orders_sent > 0 {
                    info!(
                        "[{}] Orders: sent={} accepted={} canceled={} rejected={} reprices={} timeouts={}",
                        symbol,
                        stats.orders_sent,
                        stats.orders_accepted,
                        stats.orders_canceled,
                        stats.rejections,
                        stats.reprices,
                        stats.timeouts,
                    );
                }
            }
        } else {
            info!(
                "Stats: {} messages ({:.1}/sec), {} quotes ({:.1}/sec), {} total history entries",
                self.message_count,
                rate,
                self.quote_count,
                quote_rate,
                stats.iter().map(|s| s.history_count).sum::<usize>(),
            );
        }

        self.message_count = 0;
        self.quote_count = 0;
        self.order_decision_count = 0;
        self.last_stats_time = std::time::Instant::now();
    }
}

/// Application state.
struct App {
    /// Configuration
    config: Config,
    /// Orderbook storage
    store: Arc<OrderbookStore>,
    /// OBI strategies per symbol
    strategies: HashMap<String, ObiStrategy>,
    /// Shared position per symbol (for lock-free reads)
    positions: HashMap<String, Arc<SharedPosition>>,
    /// Order managers per symbol
    order_managers: HashMap<String, QuoteOrderManager>,
    /// Channel to send order decisions to executor (uses Arc<str> for cheap clones)
    order_tx: Option<mpsc::Sender<(Arc<str>, OrderDecision)>>,
    /// Pre-allocated Arc<str> per symbol for hot path (avoids allocation on each decision)
    symbol_arcs: HashMap<String, Arc<str>>,
    /// Quote formatter
    quote_formatter: QuoteFormatter,
    /// Statistics tracker (extracted to reduce App struct size)
    stats: StatsTracker,
    /// Latest received_at timestamp from orderbook messages (nanoseconds).
    ///
    /// # Clock Source Design
    /// This uses StandX server time (`received_at` from orderbook messages) rather than
    /// local system time (`chrono::Utc::now()`). This ensures consistent timeout checking
    /// since orders are also timestamped with `received_at` when created.
    ///
    /// Note: Other components use different clock sources:
    /// - `order_manager.rs:189`: session_prefix uses local `chrono::Utc::now()` (for uniqueness only)
    /// - `position.rs:52-57`: staleness check uses local `SystemTime::now()` (acceptable for staleness)
    last_received_at_ns: i64,
}

impl App {
    /// Create a new application.
    fn new(config: Config) -> Self {
        let store = Arc::new(OrderbookStore::new(
            &config.symbols,
            config.history_buffer_size,
            config.history_minutes,
        ));

        // Create OBI strategy, shared position, order manager, and Arc<str> for each symbol
        let mut strategies = HashMap::new();
        let mut positions = HashMap::new();
        let mut order_managers = HashMap::new();
        let mut symbol_arcs = HashMap::new();

        for symbol in &config.symbols {
            let strategy = ObiStrategy::with_required_history(config.strategy.clone(), config.history_minutes);
            strategies.insert(symbol.clone(), strategy);

            let position = Arc::new(SharedPosition::new(symbol.clone()));
            positions.insert(symbol.clone(), Arc::clone(&position));

            // Create order manager config from strategy and order configs
            let om_config = OrderManagerConfig {
                symbol: symbol.clone(),
                reprice_threshold_bps: config.order.reprice_threshold_bps,
                max_position_dollar: config.strategy.max_position_dollar,
                pending_timeout_ns: config.order.pending_timeout_secs * 1_000_000_000,
                tick_size: config.strategy.tick_size,
                lot_size: config.strategy.lot_size,
                debug: config.debug,
            };
            let order_manager = QuoteOrderManager::new(om_config, position);
            order_managers.insert(symbol.clone(), order_manager);

            // Pre-allocate Arc<str> for hot path (avoids allocation per decision)
            symbol_arcs.insert(symbol.clone(), Arc::from(symbol.as_str()));
        }

        // Determine price precision from tick_size
        let tick_size = config.strategy.tick_size;
        let price_precision = if tick_size >= 1.0 {
            0
        } else {
            (-tick_size.log10().floor()) as usize
        };

        Self {
            config,
            store,
            strategies,
            positions,
            order_managers,
            order_tx: None,
            symbol_arcs,
            quote_formatter: QuoteFormatter::new(price_precision, 4),
            stats: StatsTracker::new(),
            last_received_at_ns: 0,
        }
    }

    /// Set the order decision channel.
    fn set_order_tx(&mut self, tx: mpsc::Sender<(Arc<str>, OrderDecision)>) {
        self.order_tx = Some(tx);
    }

    /// Get positions map for position poller setup.
    fn positions(&self) -> &HashMap<String, Arc<SharedPosition>> {
        &self.positions
    }

    /// Get trading stats for wallet tracker.
    fn trading_stats(&self) -> Arc<TradingStats> {
        self.stats.trading_stats()
    }

    /// Get pre-allocated Arc<str> for a symbol (hot path optimization).
    #[inline]
    fn get_symbol_arc(&self, symbol: &str) -> Arc<str> {
        self.symbol_arcs.get(symbol)
            .cloned()
            .unwrap_or_else(|| Arc::from(symbol))
    }

    /// Get mutable reference to order managers.
    fn order_managers_mut(&mut self) -> &mut HashMap<String, QuoteOrderManager> {
        &mut self.order_managers
    }

    /// Get a specific order manager by symbol (O(1) lookup).
    fn get_order_manager_mut(&mut self, symbol: &str) -> Option<&mut QuoteOrderManager> {
        self.order_managers.get_mut(symbol)
    }

    /// Check pending order timeouts for all symbols.
    /// Called periodically from the main event loop to ensure timeouts are
    /// enforced even when market data updates are sparse.
    fn check_order_timeouts(&mut self) {
        if !self.config.order.enabled {
            return;
        }

        // Skip if we haven't received any orderbook messages yet
        if self.last_received_at_ns == 0 {
            return;
        }

        // Use the same timestamp source as order creation (StandX server time)
        // This avoids clock drift issues between local system and StandX server
        let current_time_ns = self.last_received_at_ns;

        for (symbol, manager) in &mut self.order_managers {
            let timeout_decisions = manager.check_timeouts_now(current_time_ns);
            if !timeout_decisions.is_empty() {
                if let Some(tx) = &self.order_tx {
                    // Use pre-allocated Arc<str> for hot path (avoids allocation)
                    let symbol_arc = self.symbol_arcs.get(symbol)
                        .cloned()
                        .unwrap_or_else(|| Arc::from(symbol.as_str()));
                    for decision in timeout_decisions {
                        self.stats.order_decision_count += 1;
                        if tx.try_send((Arc::clone(&symbol_arc), decision)).is_err() {
                            warn!("[{}] Order decision channel full, dropping timeout cancel", symbol);
                        }
                    }
                }
            }
        }
    }

    /// Shutdown all order managers and get orders to cancel.
    fn shutdown_order_managers(&mut self) -> Vec<String> {
        let mut all_orders = Vec::new();
        for (_, manager) in &mut self.order_managers {
            manager.shutdown();
            all_orders.extend(manager.get_all_live_order_ids());
        }
        all_orders
    }

    /// Process a WebSocket event.
    fn process_event(&mut self, event: WsEvent) {
        match event {
            WsEvent::Connected => {
                info!("Connected to StandX WebSocket");
            }
            WsEvent::Disconnected(reason) => {
                warn!("Disconnected: {}", reason);
            }
            WsEvent::Message(msg, received_at) => {
                self.process_message(msg, received_at);
            }
            WsEvent::ParseError(err) => {
                debug!("Parse error: {}", err);
            }
            WsEvent::Error(err) => {
                error!("WebSocket error: {}", err);
            }
        }
    }

    /// Process a parsed StandX message.
    fn process_message(&mut self, msg: StandXMessage, received_at: i64) {
        match msg {
            StandXMessage::DepthBook(data) => {
                self.stats.message_count += 1;
                // Update latest timestamp for timeout checking (same clock as order creation)
                self.last_received_at_ns = received_at;

                // Convert to snapshot
                match data.to_snapshot(self.config.orderbook_levels, received_at) {
                    Ok(snapshot) => {

                        // Validate orderbook integrity in debug builds only (no production latency)
                        #[cfg(debug_assertions)]
                        if self.stats.message_count % 100 == 1 {
                            if let Err(e) = snapshot.validate() {
                                error!("[{}] Orderbook validation failed: {}", data.symbol, e);
                                debug!("[{}] {}", data.symbol, snapshot.debug_levels(5));
                            }
                        }

                        // Feed snapshot to OBI strategy FIRST (uses reference only)
                        // This allows us to move the snapshot to the store afterward without cloning
                        let mut quote_result = None;
                        let mut is_warming_up = false;

                        if let Some(strategy) = self.strategies.get_mut(&data.symbol) {
                            // Update strategy position from shared atomic (lock-free read)
                            if let Some(shared_pos) = self.positions.get(&data.symbol) {
                                let pos = shared_pos.get();
                                strategy.set_position(pos);
                            }

                            if let Some(quote) = strategy.update(&snapshot) {
                                quote_result = Some(quote);
                            } else if !strategy.is_warmed_up() && self.stats.message_count % 20 == 0 {
                                is_warming_up = true;
                            }
                        }

                        // Log warmup message if needed (before moving snapshot)
                        if is_warming_up {
                            info!(
                                "[{}] Warming up... {} msgs, mid={:.2}",
                                data.symbol,
                                self.stats.message_count,
                                snapshot.mid_price().unwrap_or(0.0)
                            );
                        }

                        // Update trading stats mid_price (single atomic store ~1ns)
                        if let Some(mid) = snapshot.mid_price() {
                            self.stats.trading_stats.set_mid_price(mid);
                        }

                        // Update orderbook store (still clones internally for history,
                        // but we avoided the external clone by reordering operations)
                        if let Some(ob) = self.store.get(&data.symbol) {
                            ob.update(snapshot);
                        }

                        // Note: Timeout checking is now consolidated in the periodic 1-second timer
                        // (check_order_timeouts). This removes duplicate checking that was here before.
                        // The periodic timer uses the same StandX server timestamp for consistency.

                        // Process quote if we got one
                        if let Some(quote) = quote_result {
                            // Log quote using formatter
                            self.quote_formatter.log_quote(&quote);
                            self.stats.quote_count += 1;

                            // Process quote through order manager if enabled
                            if self.config.order.enabled {
                                if let Some(order_manager) = self.order_managers.get_mut(&data.symbol) {
                                    let decisions = order_manager.on_quote(&quote, received_at);

                                    // Send decisions to executor via channel
                                    if let Some(tx) = &self.order_tx {
                                        // Use pre-allocated Arc<str> for hot path (avoids allocation)
                                        let symbol_arc = self.get_symbol_arc(&data.symbol);
                                        for decision in decisions {
                                            self.stats.order_decision_count += 1;
                                            // Non-blocking send (Arc::clone is just a refcount increment)
                                            if tx.try_send((Arc::clone(&symbol_arc), decision)).is_err() {
                                                warn!("[{}] Order decision channel full, dropping decision", data.symbol);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Log periodic orderbook updates if verbose
                        if self.config.verbose && self.stats.message_count % 100 == 0 {
                            if let Some(ob) = self.store.get(&data.symbol) {
                                if let Some(latest) = ob.latest() {
                                    info!(
                                        "[{}] bid={:.2} ask={:.2} spread={:.2} levels={}/{}",
                                        data.symbol,
                                        latest.best_bid_price().unwrap_or(0.0),
                                        latest.best_ask_price().unwrap_or(0.0),
                                        latest.spread().unwrap_or(0.0),
                                        latest.bid_count,
                                        latest.ask_count,
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to create snapshot: {}", e);
                    }
                }
            }
            StandXMessage::Price(data) => {
                debug!("Price update for {}: {:?}", data.symbol, data.last_price);
            }
            StandXMessage::Trade(data) => {
                debug!(
                    "Trade {} {} @ {} qty={}",
                    data.symbol,
                    if data.is_buyer_taker { "BUY" } else { "SELL" },
                    data.price,
                    data.qty
                );
            }
            StandXMessage::Auth { code, message } => {
                if code == 200 {
                    info!("Authentication successful");
                } else {
                    error!("Authentication failed: {} - {}", code, message);
                }
            }
            StandXMessage::Error { code, message } => {
                error!("Error {}: {}", code, message);
            }
            StandXMessage::Unknown(_) => {
                // Unknown message type, ignore
            }
        }
    }

    /// Log statistics (delegates to StatsTracker).
    fn log_stats(&mut self) {
        self.stats.log_if_needed(
            &self.config,
            &self.store,
            &self.strategies,
            &self.order_managers,
        );
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env for credentials
    dotenvy::dotenv().ok();

    // Load configuration first (to get debug flag)
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.json".to_string());

    let config = match Config::from_file(&config_path) {
        Ok(c) => c,
        Err(e) => {
            // Log the actual error and use defaults
            eprintln!("WARNING: Config error for '{}': {} - using defaults", config_path, e);
            Config::default()
        }
    };

    // Initialize centralized logging with debug flag from config
    init_logging(config.debug);

    info!("StandX Market Maker v{}", standx_orderbook::VERSION);

    if !config.debug {
        info!("Debug logging is disabled");
    }

    info!(
        "Config: symbols={:?}, levels={}, history={}min, buffer={}",
        config.symbols,
        config.orderbook_levels,
        config.history_minutes,
        config.history_buffer_size,
    );

    info!(
        "Strategy: tick={}, window={}, update_interval={}, vol_to_spread={}",
        config.strategy.tick_size,
        config.strategy.window_steps,
        config.strategy.update_interval_steps,
        config.strategy.vol_to_half_spread,
    );

    if config.order.enabled {
        info!(
            "Order management: ENABLED (reprice_threshold={}bps, pending_timeout={}s)",
            config.order.reprice_threshold_bps,
            config.order.pending_timeout_secs,
        );
    } else {
        info!("Order management: DISABLED (set order.enabled=true in config to enable)");
    }

    // Create application
    let mut app = App::new(config.clone());

    // Create shared auth manager if any feature needs it
    // IMPORTANT: Use a SINGLE AuthManager for all features to ensure consistent ed25519 keypair
    // When any feature calls authenticate(), it regenerates the keypair, so sharing ensures
    // all components use the same signing keys.
    let needs_auth = config.position.enabled || config.order.enabled || config.pnl_tracking.enabled;
    let shared_auth: Option<Arc<Mutex<AuthManager>>> = if needs_auth {
        match AuthManager::from_env() {
            Ok(auth) => {
                let auth = Arc::new(Mutex::new(auth));

                // Authenticate once for all features
                {
                    let mut auth_guard = auth.lock().await;
                    match auth_guard.authenticate().await {
                        Ok(token) => {
                            info!(
                                "Authenticated (expires in {} hours)",
                                token.remaining_secs() / 3600
                            );
                        }
                        Err(e) => {
                            error!("Failed to authenticate: {}", e);
                            error!("Features requiring auth will be disabled");
                        }
                    }
                }

                Some(auth)
            }
            Err(e) => {
                warn!("No credentials available: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Start position polling if enabled and auth is available
    let mut position_handles: Vec<PositionPollerHandle> = Vec::new();

    if config.position.enabled {
        if let Some(ref auth) = shared_auth {
            info!(
                "Position polling enabled (interval: {}s)",
                config.position.poll_interval_secs
            );

            // Start position poller for each symbol
            for (symbol, shared_pos) in app.positions() {
                let poller_config = PositionPollerConfig {
                    interval: Duration::from_secs(config.position.poll_interval_secs),
                    symbol: symbol.clone(),
                    stale_threshold: Duration::from_secs(config.position.stale_threshold_secs),
                };

                let poller = PositionPoller::new(
                    Arc::clone(shared_pos),
                    Arc::clone(auth),
                    poller_config,
                    app.trading_stats(),
                );

                let handle = poller.start();
                position_handles.push(handle);
            }
        } else {
            warn!("Position polling disabled (no credentials)");
        }
    } else {
        info!("Position polling disabled in config");
    }

    // Set up order management if enabled
    let mut order_event_rx: Option<mpsc::Receiver<OrderEvent>> = None;
    let order_client: Option<Arc<OrderWsClient>> = if config.order.enabled {
        if let Some(ref auth) = shared_auth {
            // Check if auth succeeded
            let auth_ok = {
                let auth_guard = auth.lock().await;
                auth_guard.is_authenticated()
            };

            if !auth_ok {
                error!("Order management disabled (authentication failed)");
                None
            } else {
                // Cancel all existing orders before starting (cleanup from previous sessions)
                {
                    let mut auth_guard = auth.lock().await;
                    match auth_guard.cancel_all_orders(None).await {
                        Ok(count) => {
                            if count > 0 {
                                info!("Canceled {} orphaned order(s) from previous session", count);
                            } else {
                                info!("No orphaned orders to cancel");
                            }
                        }
                        Err(e) => {
                            warn!("Failed to cancel existing orders on startup: {}", e);
                        }
                    }
                }

                // Create OrderWsClient with auto-reconnection using config values
                let reconnect_config = config.websocket.to_order_reconnect_config(
                    config.order.max_reconnect_attempts
                );
                let ws_client = Arc::new(OrderWsClient::with_config(
                    Arc::clone(auth),
                    &config.websocket.api_url,
                    reconnect_config,
                ));
                let rx = Arc::clone(&ws_client).run().await;
                let max_retries_msg = if config.order.max_reconnect_attempts == 0 {
                    "unlimited".to_string()
                } else {
                    format!("max {} retries", config.order.max_reconnect_attempts)
                };
                info!("Order WebSocket client started with auto-reconnection ({})", max_retries_msg);
                order_event_rx = Some(rx);

                // Create order decision channel (uses Arc<str> for cheap symbol clones)
                let (order_tx, order_rx) = mpsc::channel::<(Arc<str>, OrderDecision)>(1000);
                app.set_order_tx(order_tx);

                let executor_client = Arc::clone(&ws_client);

                // Capture precision params for executor (avoids config access in async task)
                let tick_size = app.config.strategy.tick_size;
                let lot_size = app.config.strategy.lot_size;

                // Spawn order executor task
                tokio::spawn(async move {
                    let mut order_rx = order_rx;
                    info!("Order executor task started");
                    while let Some((symbol, decision)) = order_rx.recv().await {
                        match decision {
                            OrderDecision::Send { side, price, qty, cl_ord_id } => {
                                let req = match side {
                                    // Dereference Arc<str> to &str for Into<String>
                                    Side::Buy => NewOrderRequest::post_only_buy_with_precision(&*symbol, price, qty, tick_size, lot_size)
                                        .with_client_id(&cl_ord_id),
                                    Side::Sell => NewOrderRequest::post_only_sell_with_precision(&*symbol, price, qty, tick_size, lot_size)
                                        .with_client_id(&cl_ord_id),
                                };
                                debug!("[{}] Sending {} order: {} @ {:.2}", symbol, side, cl_ord_id, price);
                                if let Err(e) = executor_client.place_order(req).await {
                                    warn!("[{}] Failed to place order: {}", symbol, e);
                                }
                            }
                            OrderDecision::Cancel { cl_ord_id } => {
                                debug!("[{}] Canceling order: {}", symbol, cl_ord_id);
                                if let Err(e) = executor_client.cancel_order_by_client_id(&cl_ord_id).await {
                                    warn!("[{}] Failed to cancel order: {}", symbol, e);
                                }
                            }
                            OrderDecision::CancelAndReplace { cancel_id, new_price, qty: _ } => {
                                debug!("[{}] Cancel and replace: {} -> {:.2}", symbol, cancel_id, new_price);
                                // Cancel only - new order will be placed on next quote cycle
                                if let Err(e) = executor_client.cancel_order_by_client_id(&cancel_id).await {
                                    warn!("[{}] Failed to cancel order for replacement: {}", symbol, e);
                                }
                            }
                            OrderDecision::NoAction => {}
                        }
                    }
                    info!("Order executor task stopped");
                });

                Some(ws_client)
            }
        } else {
            warn!("Order management disabled (no credentials)");
            None
        }
    } else {
        None
    };

    // Start PnL tracking if enabled (uses shared auth)
    let mut wallet_handle: Option<WalletTrackerHandle> = None;
    if config.pnl_tracking.enabled {
        if let Some(ref auth) = shared_auth {
            let tracker_config = WalletTrackerConfig {
                interval: Duration::from_secs(config.pnl_tracking.poll_interval_secs),
                csv_path: config.pnl_tracking.csv_path.clone(),
            };
            // Use first symbol's position for position_usd tracking
            // (typically only one symbol is traded)
            let position = app.positions()
                .values()
                .next()
                .cloned()
                .unwrap_or_else(|| Arc::new(SharedPosition::new("unknown".to_string())));
            let trading_stats = app.trading_stats();
            let tracker = WalletTracker::new(Arc::clone(auth), tracker_config, position, trading_stats);
            wallet_handle = Some(tracker.start());
            info!(
                "PnL tracker started (interval: {}s, csv: {})",
                config.pnl_tracking.poll_interval_secs,
                config.pnl_tracking.csv_path
            );
        } else {
            warn!("PnL tracking disabled (no credentials)");
        }
    }

    // Start orderbook sanity checker if enabled
    let mut sanity_handle: Option<SanityCheckerHandle> = None;
    if config.orderbook_sanity_check.enabled {
        let checker_config = SanityCheckerConfig::from(&config.orderbook_sanity_check);
        let checker = OrderbookSanityChecker::new(
            Arc::clone(&app.store),
            config.symbols.clone(),
            checker_config,
        );
        sanity_handle = Some(checker.start());
        info!(
            "Orderbook sanity checker started (interval: {}s, threshold: {} bps)",
            config.orderbook_sanity_check.interval_secs,
            config.orderbook_sanity_check.drift_threshold_bps
        );
    }

    // Create WebSocket client for market data
    let client = WsClientBuilder::new()
        .config(config.websocket.clone())
        .symbols(config.symbols.clone())
        .build();

    let stats = client.stats();

    // Start WebSocket client
    info!("Connecting to {}", config.websocket.url);
    let mut rx = Arc::clone(&client).run().await;

    // Main event loop
    info!("Starting event loop (press Ctrl+C to stop)");

    // Create interval OUTSIDE the loop so it persists across iterations
    // (Using sleep inside select! would recreate it every iteration, never completing)
    let mut periodic_interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            // Handle market data WebSocket events
            Some(event) = rx.recv() => {
                app.process_event(event);
            }

            // Handle order events (if order management enabled)
            // Uses guard to skip this arm entirely when order_event_rx is None
            Some(event) = async {
                order_event_rx.as_mut().unwrap().recv().await
            }, if order_event_rx.is_some() => {
                match event {
                    OrderEvent::OrderAccepted { cl_ord_id, order_id } => {
                        // O(1) lookup: extract symbol from cl_ord_id format: mm_<symbol>_<ts>_<seq>
                        match QuoteOrderManager::extract_symbol_from_cl_ord_id(&cl_ord_id) {
                            Some(symbol) => {
                                if let Some(manager) = app.get_order_manager_mut(symbol) {
                                    manager.on_order_accepted(&cl_ord_id, order_id);
                                }
                            }
                            None => {
                                warn!("Failed to extract symbol from cl_ord_id='{}', order_id={}", cl_ord_id, order_id);
                            }
                        }
                    }
                    OrderEvent::OrderRejected { cl_ord_id, reason } => {
                        // O(1) lookup: extract symbol from cl_ord_id
                        if let Some(symbol) = QuoteOrderManager::extract_symbol_from_cl_ord_id(&cl_ord_id) {
                            if let Some(manager) = app.get_order_manager_mut(symbol) {
                                manager.on_order_rejected(&cl_ord_id, &reason);
                            }
                        }
                    }
                    OrderEvent::OrderFilled { order_id, fill_qty, fill_price } => {
                        // Volume is inferred from position changes in PositionPoller
                        // Position poller remains source of truth for position
                        // Log fills for debugging
                        info!("Fill received: order_id={}, qty={}, price={}", order_id, fill_qty, fill_price);
                    }
                    OrderEvent::OrderCanceled { order_id, cl_ord_id } => {
                        // First try matching by cl_ord_id (works for orders that were never accepted)
                        if let Some(ref cl_ord_id) = cl_ord_id {
                            if let Some(symbol) = QuoteOrderManager::extract_symbol_from_cl_ord_id(cl_ord_id) {
                                if let Some(manager) = app.get_order_manager_mut(symbol) {
                                    manager.on_order_canceled_by_cl_ord_id(cl_ord_id);
                                    continue;
                                }
                            }
                        }
                        // Fallback: scan all managers by order_id
                        // This is acceptable: cancel events are infrequent
                        for (_, manager) in app.order_managers_mut() {
                            manager.on_order_canceled(order_id);
                        }
                    }
                    OrderEvent::CancelFailed { order_id, reason } => {
                        // Cancel failed - order is still live, revert state
                        for (_, manager) in app.order_managers_mut() {
                            manager.on_cancel_failed(order_id, &reason);
                        }
                    }
                    OrderEvent::Connected => {
                        info!("Order WebSocket connected");
                    }
                    OrderEvent::Authenticated => {
                        info!("Order WebSocket authenticated, resuming trading");
                        // Resume all order managers
                        for (_, manager) in app.order_managers_mut() {
                            manager.resume();
                        }
                    }
                    OrderEvent::Disconnected(reason) => {
                        warn!("Order WebSocket disconnected: {}", reason);

                        // 1. Pause all order managers IMMEDIATELY (atomic, no latency)
                        for (_, manager) in app.order_managers_mut() {
                            manager.pause();
                        }

                        // 2. Clear internal order state IMMEDIATELY
                        // This prevents stale orders from blocking new placements after reconnect
                        for (_, manager) in app.order_managers_mut() {
                            manager.clear_all_orders();
                        }

                        // 3. Cancel all orders via HTTP (reliable fallback to clean exchange side)
                        if let Some(auth) = &shared_auth {
                            let auth_clone = Arc::clone(auth);
                            // Spawn to avoid blocking main loop
                            tokio::spawn(async move {
                                let mut auth_guard = auth_clone.lock().await;
                                if let Err(e) = auth_guard.cancel_all_orders(None).await {
                                    error!("Failed to cancel orders on disconnect: {}", e);
                                } else {
                                    info!("Canceled all orders due to WebSocket disconnect");
                                }
                            });
                        }
                    }
                    OrderEvent::Reconnecting { attempt, delay_secs } => {
                        info!("Order WebSocket reconnecting in {}s (attempt {})", delay_secs, attempt);
                    }
                    OrderEvent::MaxRetriesExceeded => {
                        error!("Order WebSocket max retries exceeded, initiating shutdown");
                        // Cancel all orders via HTTP
                        if let Some(auth) = &shared_auth {
                            let mut auth_guard = auth.lock().await;
                            let _ = auth_guard.cancel_all_orders(None).await;
                        }
                        // Stop the market data WebSocket and exit
                        client.stop();
                        break;
                    }
                    OrderEvent::Error(msg) => {
                        // Expected errors when canceling timed-out orders - demote to debug
                        if msg.contains("order not found") || msg.contains("order is not open") {
                            debug!("Order WebSocket (expected): {}", msg);
                        } else {
                            error!("Order WebSocket error: {}", msg);
                        }
                    }
                }
            }

            // Handle shutdown signal
            _ = signal::ctrl_c() => {
                info!("Received shutdown signal");

                // Cancel all live orders before shutdown
                let orders_to_cancel = app.shutdown_order_managers();
                if !orders_to_cancel.is_empty() {
                    info!("Canceling {} live order(s)...", orders_to_cancel.len());

                    // Use HTTP batch cancel for reliability (reuse existing auth)
                    if let Some(auth) = &shared_auth {
                        let mut auth_guard = auth.lock().await;
                        match auth_guard.cancel_orders_by_client_id(&orders_to_cancel).await {
                            Ok(_) => info!("Successfully canceled orders on shutdown"),
                            Err(e) => error!("Failed to cancel orders on shutdown: {}", e),
                        }
                    }

                    // Wait a bit for cancels to process
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }

                // Disconnect order WebSocket
                if let Some(ref oc) = order_client {
                    oc.disconnect().await;
                }

                // Stop sanity checker
                if let Some(handle) = sanity_handle.take() {
                    info!("Stopping sanity checker...");
                    handle.stop();
                }

                // Stop wallet tracker
                if let Some(handle) = wallet_handle.take() {
                    info!("Stopping wallet tracker...");
                    handle.stop();
                }

                client.stop();
                break;
            }

            // Periodic tasks: timeout checking and stats logging (runs every 1 second)
            _ = periodic_interval.tick() => {
                // Check pending order timeouts (independent of market data)
                app.check_order_timeouts();
                app.log_stats();
            }
        }
    }

    // Stop position pollers
    if !position_handles.is_empty() {
        info!("Stopping {} position poller(s)...", position_handles.len());
        for handle in position_handles {
            handle.stop();
        }
    }

    // Final stats
    let ws_stats = stats.snapshot();
    info!(
        "Final stats: {} messages received, {} reconnects, {} bytes",
        ws_stats.messages_received,
        ws_stats.reconnect_count,
        ws_stats.bytes_received,
    );

    // Log orderbook stats
    for stat in app.store.stats() {
        info!(
            "[{}] Final: {} updates, {} history snapshots",
            stat.symbol,
            stat.update_count,
            stat.history_total_writes,
        );
    }

    // Log final position data
    for (symbol, pos) in app.positions() {
        let position = pos.get();
        if position.abs() > 1e-8 {
            info!("[{}] Final position: {:.6}", symbol, position);
        }
    }

    info!("Shutdown complete");
    Ok(())
}
