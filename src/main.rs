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
    Config, OrderbookSnapshot, OrderbookStore, WsClientBuilder, WsEvent, StandXMessage,
    ObiStrategy, QuoteFormatter, init_logging, QuoteStrategy,
    AuthManager, SharedPosition, PositionPoller, PositionPollerConfig, PositionPollerHandle,
    QuoteOrderManager, OrderManagerConfig, OrderDecision, PauseReason, Side,
    WalletTracker, WalletTrackerConfig, WalletTrackerHandle,
    OrderbookSanityChecker, SanityCheckerConfig, SanityCheckerHandle,
    OpenOrdersChecker, OpenOrdersCheckerConfig, OpenOrdersCheckerHandle, ClearOrdersSignal,
    OpenOrdersSnapshot,
    SharedSymbolInfo, SymbolInfoPoller, SymbolInfoPollerConfig, SymbolInfoPollerHandle, TickSizeChangedSignal,
    SharedAlpha, BinanceAlphaPollerHandle, start_binance_alpha_poller,
    SharedBbo, BinanceBboPollerHandle, start_binance_bbo_poller,
};
use standx_orderbook::trading::SharedEquity;
use standx_orderbook::trading::TradingStats;
use standx_orderbook::trading::{OrderWsClient, OrderEvent, NewOrderRequest, StandXClient};

#[derive(Debug)]
struct ReconciliationResult {
    generation: u64,
    reason: String,
    attempts: u32,
}

#[derive(Debug)]
struct ReconciliationRequest {
    generation: u64,
    reason: String,
}

async fn run_reconciliation_worker(
    auth: Arc<Mutex<AuthManager>>,
    mut requests: mpsc::UnboundedReceiver<ReconciliationRequest>,
    results: mpsc::Sender<ReconciliationResult>,
) {
    while let Some(request) = requests.recv().await {
        let mut generation = request.generation;
        let mut reason = request.reason;
        while let Ok(next_request) = requests.try_recv() {
            reason.push_str("; ");
            reason.push_str(&next_request.reason);
            generation = generation.max(next_request.generation);
        }

        let mut attempt = 0_u32;
        loop {
            attempt = attempt.saturating_add(1);
            let outcome = {
                let mut auth = auth.lock().await;
                match auth.cancel_all_orders(None).await {
                    Ok(_) => auth
                        .query_open_orders(None)
                        .await
                        .map(|orders| orders.is_empty()),
                    Err(error) => Err(error),
                }
            };

            match outcome {
                Ok(true) => {
                    let _ = results
                        .send(ReconciliationResult {
                            generation,
                            reason: reason.clone(),
                            attempts: attempt,
                        })
                        .await;
                    break;
                }
                Ok(false) => {
                    error!(
                        "Reconciliation attempt {} left open orders; trading remains paused",
                        attempt
                    );
                }
                Err(error) => {
                    error!(
                        "Reconciliation attempt {} failed: {}; trading remains paused",
                        attempt, error
                    );
                }
            }

            let delay_secs = (1_u64 << attempt.min(5)).min(30);
            tokio::time::sleep(Duration::from_secs(delay_secs)).await;
        }
    }
}

/// Account-wide cleanup used at lifecycle boundaries. Success means the REST
/// open-order query returned an empty account after cancellation.
async fn cancel_all_and_verify(
    auth: &Arc<Mutex<AuthManager>>,
    context: &str,
    max_attempts: u32,
) -> bool {
    for attempt in 1..=max_attempts {
        let outcome = {
            let mut auth = auth.lock().await;
            match auth.cancel_all_orders(None).await {
                Ok(canceled) => auth
                    .query_open_orders(None)
                    .await
                    .map(|remaining| (canceled, remaining.len())),
                Err(error) => Err(error),
            }
        };

        match outcome {
            Ok((canceled, 0)) => {
                info!(
                    "{} cleanup verified: canceled {} order(s), zero account orders remain",
                    context, canceled
                );
                return true;
            }
            Ok((canceled, remaining)) => {
                error!(
                    "{} cleanup attempt {}/{} canceled {} order(s), but {} remain",
                    context, attempt, max_attempts, canceled, remaining
                );
            }
            Err(error) => {
                error!(
                    "{} cleanup attempt {}/{} failed: {}",
                    context, attempt, max_attempts, error
                );
            }
        }

        if attempt < max_attempts {
            tokio::time::sleep(Duration::from_secs(1_u64 << (attempt - 1).min(3))).await;
        }
    }

    error!(
        "CRITICAL: {} cleanup could not verify zero open account orders",
        context
    );
    false
}

async fn log_account_positions(auth: &Arc<Mutex<AuthManager>>, context: &str) {
    let result = auth.lock().await.query_positions(None).await;
    match result {
        Ok(positions) if positions.is_empty() => info!("{} position: flat", context),
        Ok(positions) => {
            for position in positions {
                info!(
                    "{} position: symbol={}, qty={}, status={}, entry_price={:?}, mark_price={:?}",
                    context,
                    position.symbol,
                    position.qty,
                    position.status,
                    position.entry_price,
                    position.mark_price
                );
            }
        }
        Err(error) => error!("Failed to query {} position: {}", context, error),
    }
}

/// Statistics tracker for monitoring throughput and rates.
/// Extracted from App to reduce struct bloat and separate concerns.
struct StatsTracker {
    /// Message counter for logging
    message_count: u64,
    /// Quote counter
    quote_count: u64,
    /// Order decision counter
    order_decision_count: u64,
    /// Consecutive order decision channel drops (reset on successful send)
    consecutive_channel_drops: u64,
    /// Last stats log time
    last_stats_time: std::time::Instant,
    /// Last position log time
    last_position_log: std::time::Instant,
    /// Shared trading stats for volume/price tracking (used by WalletTracker)
    trading_stats: Arc<TradingStats>,
}

impl StatsTracker {
    fn new() -> Self {
        Self {
            message_count: 0,
            quote_count: 0,
            order_decision_count: 0,
            consecutive_channel_drops: 0,
            last_stats_time: std::time::Instant::now(),
            last_position_log: std::time::Instant::now(),
            trading_stats: Arc::new(TradingStats::default()),
        }
    }

    /// Get trading stats for wallet tracker.
    fn trading_stats(&self) -> Arc<TradingStats> {
        Arc::clone(&self.trading_stats)
    }

    /// Log statistics if interval has elapsed.
    #[allow(clippy::too_many_arguments)]
    fn log_if_needed(
        &mut self,
        config: &Config,
        store: &OrderbookStore,
        strategies: &HashMap<String, ObiStrategy>,
        order_managers: &HashMap<String, QuoteOrderManager>,
        positions: &HashMap<String, Arc<SharedPosition>>,
        order_ws_connected: bool,
        order_ws_authenticated: bool,
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
            // Build WebSocket state string
            let ws_state = if order_ws_authenticated {
                "WS: connected+auth"
            } else if order_ws_connected {
                "WS: connected (auth pending)"
            } else {
                "WS: DISCONNECTED"
            };

            info!(
                "Stats: {} messages ({:.1}/sec), {} quotes ({:.1}/sec), {} orders ({:.1}/sec) | {}",
                self.message_count,
                rate,
                self.quote_count,
                quote_rate,
                self.order_decision_count,
                order_rate,
                ws_state,
            );

            // Log order manager stats
            for (symbol, manager) in order_managers {
                let stats = manager.stats();
                let paused = manager.is_paused();
                let rejections = manager.consecutive_rejections();

                // Build status string
                let status = if paused {
                    if rejections > 0 {
                        format!("PAUSED (circuit breaker: {} rejections)", rejections)
                    } else {
                        "PAUSED (WS disconnected)".to_string()
                    }
                } else {
                    "TRADING".to_string()
                };

                // Build live orders string
                let bid_info = manager.bid_order()
                    .map(|o| format!("bid@{:.2}", o.price))
                    .unwrap_or_else(|| "no bid".to_string());
                let ask_info = manager.ask_order()
                    .map(|o| format!("ask@{:.2}", o.price))
                    .unwrap_or_else(|| "no ask".to_string());

                // Always log status even if no orders sent (to catch silent stops)
                info!(
                    "[{}] Status: {} | Live: {}, {} | Orders: sent={} accepted={} rejected={} timeouts={}",
                    symbol,
                    status,
                    bid_info,
                    ask_info,
                    stats.orders_sent,
                    stats.orders_accepted,
                    stats.rejections,
                    stats.timeouts,
                );

                // Health warning: log if no orders sent for a long time while not paused
                // This helps catch silent failures where trading stopped without obvious error
                const IDLE_WARNING_SECS: u64 = 300; // 5 minutes
                if !paused {
                    if let Some(idle_secs) = manager.seconds_since_last_order() {
                        if idle_secs > IDLE_WARNING_SECS {
                            warn!(
                                "[{}] HEALTH WARNING: No orders sent for {} seconds while not paused!",
                                symbol, idle_secs
                            );
                        }
                    }
                }
            }
        } else {
            warn!(
                "TEST/OBSERVE-ONLY MODE: {} messages ({:.1}/sec), {} quotes ({:.1}/sec), \
                 {} total history entries; order.enabled=false, so no orders will be placed",
                self.message_count,
                rate,
                self.quote_count,
                quote_rate,
                stats.iter().map(|s| s.history_count).sum::<usize>(),
            );
        }

        if self.last_position_log.elapsed() >= Duration::from_secs(60) {
            for (symbol, pos) in positions {
                let value = pos.get();
                let age_ms = pos.age_ms();
                info!("[{}] Position: {:.4} (age={}ms)", symbol, value, age_ms);
            }
            self.last_position_log = std::time::Instant::now();
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
    /// Shared symbol info per symbol (for dynamic tick_size/lot_size)
    symbol_infos: HashMap<String, Arc<SharedSymbolInfo>>,
    /// Shared equity for automatic order sizing (lock-free reads)
    shared_equity: Arc<SharedEquity>,
    /// Shared Binance BBO for lock-free reads (optional)
    #[allow(dead_code)]
    shared_binance_bbo: Option<Arc<SharedBbo>>,
    /// Order managers per symbol
    order_managers: HashMap<String, QuoteOrderManager>,
    /// Channel to send order decisions to executor (uses Arc<str> for cheap clones)
    order_tx: Option<mpsc::Sender<(Arc<str>, OrderDecision)>>,
    /// Non-blocking diagnostic history channel; consumed off the quote path.
    history_tx: mpsc::Sender<OrderbookSnapshot>,
    /// History samples dropped because the cold-path recorder was saturated.
    history_channel_drops: u64,
    /// Cold-path account-wide reconciliation trigger.
    reconciliation_tx: Option<mpsc::UnboundedSender<ReconciliationRequest>>,
    reconciliation_generation: u64,
    /// Pre-allocated Arc<str> per symbol for hot path (avoids allocation on each decision)
    symbol_arcs: HashMap<String, Arc<str>>,
    /// Reused decision buffers per symbol; no per-quote allocation.
    decision_buffers: HashMap<String, Vec<OrderDecision>>,
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
    /// Order WebSocket connection state (for stats logging)
    order_ws_connected: bool,
    /// Order WebSocket authenticated state (for stats logging)
    order_ws_authenticated: bool,
}

impl App {
    /// Create a new application with shared symbol info for dynamic tick_size/lot_size.
    fn new(
        config: Config,
        symbol_infos: HashMap<String, Arc<SharedSymbolInfo>>,
        shared_binance_alpha: Option<Arc<SharedAlpha>>,
        shared_binance_bbo: Option<Arc<SharedBbo>>,
    ) -> Self {
        let store = Arc::new(OrderbookStore::new(
            &config.symbols,
            config.history_buffer_size,
            config.history_minutes,
        ));
        let (history_tx, mut history_rx) = mpsc::channel::<OrderbookSnapshot>(4096);
        let history_store = Arc::clone(&store);
        tokio::spawn(async move {
            while let Some(snapshot) = history_rx.recv().await {
                if let Some(orderbook) = history_store.get(snapshot.symbol.as_str()) {
                    orderbook.record_history(snapshot);
                }
            }
        });

        // Create SharedEquity for automatic order sizing
        let shared_equity = Arc::new(SharedEquity::new(
            config.strategy.order_levels,
            config.strategy.min_order_qty_dollar,
            config.strategy.leverage,
        ));

        // Create OBI strategy, shared position, order manager, and Arc<str> for each symbol
        let mut strategies = HashMap::new();
        let mut positions = HashMap::new();
        let mut order_managers = HashMap::new();
        let mut symbol_arcs = HashMap::new();
        let mut decision_buffers = HashMap::new();

        for symbol in &config.symbols {
            // Create strategy with shared symbol info, shared equity, and optional Binance alpha
            let strategy = if let Some(shared_info) = symbol_infos.get(symbol) {
                if let Some(ref binance_alpha) = shared_binance_alpha {
                    // Full constructor with Binance alpha
                    ObiStrategy::with_binance_alpha(
                        config.strategy.clone(),
                        Arc::clone(shared_info),
                        Arc::clone(&shared_equity),
                        Arc::clone(binance_alpha),
                        config.history_minutes,
                    )
                } else {
                    // Without Binance alpha
                    ObiStrategy::with_shared_info(
                        config.strategy.clone(),
                        Arc::clone(shared_info),
                        Arc::clone(&shared_equity),
                        config.history_minutes,
                    )
                }
            } else {
                // Fallback to config-based tick_size (still uses shared_equity)
                ObiStrategy::with_required_history(
                    config.strategy.clone(),
                    Arc::clone(&shared_equity),
                    config.history_minutes,
                )
            };
            strategies.insert(symbol.clone(), strategy);

            let position = Arc::new(SharedPosition::new(symbol.clone()));
            positions.insert(symbol.clone(), Arc::clone(&position));

            // Get tick_size and lot_size from SharedSymbolInfo if available, else from config
            let (tick_size, lot_size) = if let Some(info) = symbol_infos.get(symbol) {
                (info.tick_size(), info.lot_size())
            } else {
                (config.strategy.tick_size, config.strategy.lot_size)
            };

            // Create order manager config from strategy and order configs
            let om_config = OrderManagerConfig {
                symbol: symbol.clone(),
                reprice_threshold_bps: config.order.reprice_threshold_bps,
                pending_timeout_ns: config.order.pending_timeout_secs * 1_000_000_000,
                max_live_age_ns: config.order.max_live_age_secs * 1_000_000_000,
                tick_size,
                lot_size,
                debug: config.debug,
                circuit_breaker_rejections: config.order.circuit_breaker_rejections,
                num_levels: config.strategy.order_levels,
                absolute_max_position_dollar: config.order.absolute_max_position_dollar,
                position_limit_from_start: config.order.position_limit_from_start,
                max_order_qty_dollar: config.strategy.max_order_qty_dollar,
            };
            let order_manager = QuoteOrderManager::new(om_config, position, Arc::clone(&shared_equity));
            if config.order.enabled {
                order_manager.set_pause_reason(PauseReason::OrderWebSocket);
                order_manager.set_pause_reason(PauseReason::MarketData);
                order_manager.set_pause_reason(PauseReason::RiskData);
            }
            order_managers.insert(symbol.clone(), order_manager);

            // Pre-allocate Arc<str> for hot path (avoids allocation per decision)
            symbol_arcs.insert(symbol.clone(), Arc::from(symbol.as_str()));
            decision_buffers.insert(symbol.clone(), Vec::with_capacity(8));
        }

        // Determine price precision from tick_size (use first symbol's info if available)
        let tick_size = symbol_infos.values().next()
            .map(|info| info.tick_size())
            .unwrap_or(config.strategy.tick_size);
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
            symbol_infos,
            shared_equity,
            shared_binance_bbo,
            order_managers,
            order_tx: None,
            history_tx,
            history_channel_drops: 0,
            reconciliation_tx: None,
            reconciliation_generation: 0,
            symbol_arcs,
            decision_buffers,
            quote_formatter: QuoteFormatter::new(price_precision, 4),
            stats: StatsTracker::new(),
            last_received_at_ns: 0,
            order_ws_connected: false,
            order_ws_authenticated: false,
        }
    }

    /// Get symbol infos map for poller setup.
    fn symbol_infos(&self) -> &HashMap<String, Arc<SharedSymbolInfo>> {
        &self.symbol_infos
    }

    /// Reset strategy for a symbol (clears rolling stats, re-enters warmup).
    fn reset_strategy(&mut self, symbol: &str) {
        if let Some(strategy) = self.strategies.get_mut(symbol) {
            strategy.reset();
            info!("[{}] Strategy reset - re-entering warmup period", symbol);
        }
    }

    /// Refresh cold-path risk freshness and publish only pause bits to quoting.
    /// Returns true when previously-fresh data becomes stale and reconciliation
    /// must be scheduled.
    fn update_risk_pauses(&mut self) -> bool {
        if !self.config.order.enabled {
            return false;
        }

        let equity_fresh = self.shared_equity.is_initialized()
            && self.shared_equity.age_ms()
                <= self.config.pnl_tracking.stale_threshold_secs.saturating_mul(1000);
        let mut became_stale = false;

        for (symbol, manager) in &mut self.order_managers {
            let position_fresh = self
                .positions
                .get(symbol)
                .map(|position| {
                    !position.is_stale(
                        self.config.position.stale_threshold_secs.saturating_mul(1000),
                    )
                })
                .unwrap_or(false);
            let sources_fresh = equity_fresh && position_fresh;
            let fill_position_current = manager.position_refresh_after_fill_complete();
            let was_stale = manager.has_pause_reason(PauseReason::RiskData);
            if sources_fresh && fill_position_current {
                manager.initialize_position_baseline();
                manager.clear_pause_reason(PauseReason::RiskData);
            } else {
                manager.set_pause_reason(PauseReason::RiskData);
                if !sources_fresh && !was_stale {
                    manager.begin_reconciliation();
                    became_stale = true;
                    error!(
                        "[{}] Risk data became stale (position_fresh={}, equity_fresh={})",
                        symbol, position_fresh, equity_fresh
                    );
                } else if sources_fresh && !fill_position_current {
                    debug!(
                        "[{}] Waiting for post-fill position confirmation",
                        symbol
                    );
                }
            }
        }

        became_stale
    }

    /// Set the order decision channel.
    fn set_order_tx(&mut self, tx: mpsc::Sender<(Arc<str>, OrderDecision)>) {
        self.order_tx = Some(tx);
    }

    fn set_reconciliation_tx(&mut self, tx: mpsc::UnboundedSender<ReconciliationRequest>) {
        self.reconciliation_tx = Some(tx);
    }

    /// Pause every manager before scheduling account-wide cleanup.
    fn begin_account_reconciliation(&mut self, reason: impl Into<String>) {
        for manager in self.order_managers.values_mut() {
            // Any cleanup can race an execution. Require two successful
            // position polls after reconciliation begins before quotes resume.
            // Do not advance the gate again for duplicate signals belonging to
            // the same in-flight reconciliation.
            if !manager.has_pause_reason(PauseReason::Reconciliation) {
                manager.require_position_refresh_after_fill();
            }
            manager.begin_reconciliation();
        }
        let reason = reason.into();
        self.reconciliation_generation = self.reconciliation_generation.wrapping_add(1);
        let request = ReconciliationRequest {
            generation: self.reconciliation_generation,
            reason,
        };
        match &self.reconciliation_tx {
            Some(tx) => {
                if let Err(error) = tx.send(request) {
                    error!("Failed to schedule reconciliation: {}", error);
                }
            }
            None => error!("Reconciliation requested without an active worker"),
        }
    }

    fn finish_account_reconciliation(&mut self) {
        for manager in self.order_managers.values_mut() {
            manager.finish_reconciliation();
        }
    }

    fn reconciliation_generation(&self) -> u64 {
        self.reconciliation_generation
    }

    /// Get positions map for position poller setup.
    fn positions(&self) -> &HashMap<String, Arc<SharedPosition>> {
        &self.positions
    }

    /// Get trading stats for wallet tracker.
    fn trading_stats(&self) -> Arc<TradingStats> {
        self.stats.trading_stats()
    }

    /// Get shared equity for wallet tracker.
    fn shared_equity(&self) -> Arc<SharedEquity> {
        Arc::clone(&self.shared_equity)
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

    /// Promote locally pending orders only when an authenticated REST snapshot
    /// contains the exact client ID. This runs from the one-second cold path.
    fn apply_open_orders_snapshot(
        &mut self,
        snapshot: &OpenOrdersSnapshot,
    ) -> Result<usize, String> {
        let manager = self
            .order_managers
            .get_mut(&snapshot.symbol)
            .ok_or_else(|| format!("no order manager for REST symbol {}", snapshot.symbol))?;
        if manager.has_pause_reason(PauseReason::Reconciliation) {
            return Ok(0);
        }
        let mut confirmed = 0usize;
        let observed_order_ids: Vec<i64> = snapshot
            .orders
            .iter()
            .map(|order| order.order_id)
            .collect();
        let observed_cl_ord_ids: Vec<&str> = snapshot
            .orders
            .iter()
            .filter_map(|order| order.cl_ord_id.as_deref())
            .collect();

        if let Some((cl_ord_id, order_id)) =
            manager.first_missing_rest_order(&observed_order_ids)
        {
            manager.require_position_refresh_after_fill();
            return Err(format!(
                "REST no longer reports tracked order {} ({}) for {}; possible fill",
                order_id, cl_ord_id, snapshot.symbol
            ));
        }
        const REST_CONFIRMATION_GRACE_NS: i64 = 9_000_000_000;
        if let Some(cl_ord_id) = manager.first_stale_unconfirmed_rest_order(
            &observed_cl_ord_ids,
            snapshot.observed_at_ns,
            REST_CONFIRMATION_GRACE_NS,
        ) {
            manager.require_position_refresh_after_fill();
            return Err(format!(
                "REST did not confirm accepted order {} for {} within 9s; possible fill or rejection",
                cl_ord_id, snapshot.symbol
            ));
        }

        for observed in &snapshot.orders {
            let cl_ord_id = observed.cl_ord_id.as_deref().ok_or_else(|| {
                format!(
                    "REST open order {} for {} omitted its client ID",
                    observed.order_id, snapshot.symbol
                )
            })?;
            // A snapshot can have been polled just before a correlated cancel
            // response freed the slot. Ignore that short-lived stale view.
            if !manager.has_tracked_order(cl_ord_id) {
                debug!(
                    "[{}] REST snapshot still contains untracked order {} ({})",
                    snapshot.symbol, observed.order_id, cl_ord_id
                );
                continue;
            }
            if let Some(fill_qty) = observed.fill_qty.as_deref() {
                let parsed_fill = fill_qty.parse::<f64>().map_err(|error| {
                    manager.require_position_refresh_after_fill();
                    format!(
                        "REST open order {} for {} has invalid fill quantity {:?}: {}",
                        observed.order_id, snapshot.symbol, fill_qty, error
                    )
                })?;
                if !parsed_fill.is_finite() || parsed_fill < 0.0 {
                    manager.require_position_refresh_after_fill();
                    return Err(format!(
                        "REST open order {} for {} has unsafe fill quantity {:?}",
                        observed.order_id, snapshot.symbol, fill_qty
                    ));
                }
                if parsed_fill > 0.0 {
                    manager.require_position_refresh_after_fill();
                    return Err(format!(
                        "REST detected partial fill {} on order {} ({}) for {}",
                        fill_qty, observed.order_id, cl_ord_id, snapshot.symbol
                    ));
                }
            }
            if !manager.on_order_accepted(cl_ord_id, observed.order_id) {
                return Err(format!(
                    "REST found untracked or conflicting order {} ({}) for {}",
                    observed.order_id, cl_ord_id, snapshot.symbol
                ));
            }
            confirmed += 1;
        }

        Ok(confirmed)
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

        // Use system time for timeout checks to avoid false timeouts when
        // orderbook messages are sparse. Clock drift is acceptable for 30s timeouts.
        use std::time::{SystemTime, UNIX_EPOCH};
        let current_time_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        // Log clock drift periodically (every ~30s based on stats interval)
        let drift_ms = (current_time_ns - self.last_received_at_ns) / 1_000_000;
        if drift_ms.abs() > 5000 {
            // Only log if drift > 5 seconds (significant)
            debug!(
                "Clock drift: system_time - server_time = {}ms",
                drift_ms
            );
        }

        let mut dispatch_failed = false;
        let mut timed_out = false;
        for (symbol, manager) in &mut self.order_managers {
            let timeout_decisions = manager.check_timeouts_now(current_time_ns);
            if !timeout_decisions.is_empty() {
                timed_out = true;
                if let Some(tx) = &self.order_tx {
                    // Use pre-allocated Arc<str> for hot path (avoids allocation)
                    let symbol_arc = self.symbol_arcs.get(symbol)
                        .cloned()
                        .unwrap_or_else(|| Arc::from(symbol.as_str()));
                    for decision in timeout_decisions {
                        self.stats.order_decision_count += 1;
                        if let Err(error) = tx.try_send((Arc::clone(&symbol_arc), decision)) {
                            let (_, failed_decision) = error.into_inner();
                            manager.on_dispatch_failed(&failed_decision);
                            manager.trigger_safety_pause("timeout cancel could not reach executor");
                            dispatch_failed = true;
                            error!("[{}] Timeout cancel could not reach executor", symbol);
                        }
                    }
                } else {
                    for decision in timeout_decisions {
                        manager.on_dispatch_failed(&decision);
                    }
                    manager.trigger_safety_pause("timeout cancel has no executor");
                    dispatch_failed = true;
                    error!("[{}] Timeout cancel has no executor", symbol);
                }
            }
        }
        if dispatch_failed {
            self.begin_account_reconciliation("timeout cancel could not reach executor");
        } else if timed_out {
            // A timeout means the exchange outcome is unknown. The manager may
            // clear its local slot, so pause before another quote event can use
            // that slot and prove account-wide cleanup through REST.
            self.begin_account_reconciliation("order or cancel response timed out");
        }
    }

    /// Check circuit breaker auto-recovery for all order managers.
    fn check_circuit_breaker_recovery(&mut self) {
        if !self.config.order.enabled || self.config.order.circuit_breaker_recovery_secs == 0 {
            return;
        }

        for manager in self.order_managers.values_mut() {
            manager.check_circuit_breaker_recovery(self.config.order.circuit_breaker_recovery_secs);
        }
    }

    /// Check safety pause auto-recovery for all order managers.
    /// This is separate from circuit breaker recovery and handles the >2 orders case.
    fn check_safety_pause_recovery(&mut self, recovery_secs: u64) {
        if !self.config.order.enabled || recovery_secs == 0 {
            return;
        }

        for manager in self.order_managers.values_mut() {
            manager.check_safety_pause_recovery(recovery_secs);
        }
    }

    /// Shutdown all order managers and get orders to cancel.
    fn shutdown_order_managers(&mut self) -> Vec<String> {
        let mut all_orders = Vec::new();
        for manager in self.order_managers.values_mut() {
            manager.shutdown();
            all_orders.extend(manager.get_all_live_order_ids());
        }
        all_orders
    }

    /// Process a WebSocket event.
    fn process_event(&mut self, event: WsEvent) -> bool {
        match event {
            WsEvent::Connected => {
                info!("Connected to StandX WebSocket");
                false
            }
            WsEvent::Disconnected(reason) => {
                warn!("Disconnected: {}", reason);
                self.last_received_at_ns = 0;
                for strategy in self.strategies.values_mut() {
                    strategy.reset();
                }
                for manager in self.order_managers.values_mut() {
                    manager.set_pause_reason(PauseReason::MarketData);
                    manager.begin_reconciliation();
                }
                true
            }
            WsEvent::Message(msg, received_at) => {
                self.process_message(msg, received_at);
                false
            }
            WsEvent::ParseError(err) => {
                debug!("Parse error: {}", err);
                false
            }
            WsEvent::Error(err) => {
                error!("WebSocket error: {}", err);
                false
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
                            } else if !strategy.is_warmed_up()
                                && self.stats.message_count.is_multiple_of(20)
                            {
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

                        // Note: Timeout checking is now consolidated in the periodic 1-second timer
                        // (check_order_timeouts). Both order creation and timeout checks use system
                        // time to avoid false timeouts when orderbook messages are sparse.

                        // Process quote if we got one
                        if let Some(quote) = quote_result {
                            self.stats.quote_count += 1;
                            let mut dispatch_failed = false;

                            // Process quote through order manager if enabled
                            if self.config.order.enabled
                                && quote.valid_for_trading
                                && quote.mid_price.is_finite()
                                && quote.mid_price > 0.0
                            {
                                let symbol_arc = self.get_symbol_arc(&data.symbol);
                                if let Some(order_manager) = self.order_managers.get_mut(&data.symbol) {
                                    // `valid_for_trading` includes the full configured history
                                    // duration (10 minutes in the live/canary configurations).
                                    order_manager.clear_pause_reason(PauseReason::MarketData);
                                    // Use system time for order creation to match timeout checks
                                    use std::time::{SystemTime, UNIX_EPOCH};
                                    let system_time_ns = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_nanos() as i64;
                                    let decisions = self
                                        .decision_buffers
                                        .get_mut(&data.symbol)
                                        .expect("decision buffer created for every configured symbol");
                                    order_manager.on_quote_into(
                                        &quote,
                                        system_time_ns,
                                        decisions,
                                    );

                                    // Send decisions to executor via channel
                                    if let Some(tx) = &self.order_tx {
                                        for decision in decisions.drain(..) {
                                            self.stats.order_decision_count += 1;
                                            // Non-blocking send (Arc::clone is just a refcount increment)
                                            if let Err(error) = tx.try_send((Arc::clone(&symbol_arc), decision)) {
                                                let (_, failed_decision) = error.into_inner();
                                                order_manager.on_dispatch_failed(&failed_decision);
                                                self.stats.consecutive_channel_drops += 1;
                                                dispatch_failed = true;
                                                error!("[{}] Order executor channel unavailable", data.symbol);
                                            } else {
                                                self.stats.consecutive_channel_drops = 0;
                                            }
                                        }
                                    } else if !decisions.is_empty() {
                                        for decision in decisions.drain(..) {
                                            order_manager.on_dispatch_failed(&decision);
                                        }
                                        order_manager.trigger_safety_pause(
                                            "order decision has no executor",
                                        );
                                        dispatch_failed = true;
                                        error!("[{}] Order decision has no executor", data.symbol);
                                    }
                                    decisions.clear();
                                }
                            }

                            if dispatch_failed {
                                for manager in self.order_managers.values_mut() {
                                    manager.trigger_safety_pause("executor channel unavailable");
                                }
                                self.begin_account_reconciliation(
                                    "order decision could not reach executor",
                                );
                            }

                            // Log quote after trading decisions are dispatched so formatting
                            // never sits in front of the order hot path.
                            self.quote_formatter.log_quote(&quote);
                        }

                        // Storage and diagnostic history are cold-path work. Publish only
                        // after order decisions have been sent to the executor.
                        if let Some(ob) = self.store.get(&data.symbol) {
                            ob.update_current(snapshot.clone());
                        }
                        if let Err(error) = self.history_tx.try_send(snapshot) {
                            self.history_channel_drops = self.history_channel_drops.saturating_add(1);
                            if self.history_channel_drops <= 3
                                || self.history_channel_drops.is_power_of_two()
                            {
                                warn!(
                                    "[{}] History recorder unavailable (drops={}): {}",
                                    data.symbol, self.history_channel_drops, error
                                );
                            }
                        }

                        // Log periodic orderbook updates if verbose
                        if self.config.verbose && self.stats.message_count.is_multiple_of(100) {
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
            &self.positions,
            self.order_ws_connected,
            self.order_ws_authenticated,
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
            eprintln!("FATAL: Config error for '{}': {}", config_path, e);
            eprintln!("Fix the config file or provide a valid path. Refusing to start with defaults.");
            std::process::exit(1);
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
        "Strategy: tick={}, window={}, update_interval={}, vol_to_spread={}, order_levels={}, leverage={}x",
        config.strategy.tick_size,
        config.strategy.window_steps,
        config.strategy.update_interval_steps,
        config.strategy.vol_to_half_spread,
        config.strategy.order_levels,
        config.strategy.leverage,
    );

    if config.strategy.order_levels > 1 {
        info!(
            "Multi-level mode: {} levels per side, spread_level_multiplier={}",
            config.strategy.order_levels,
            config.strategy.spread_level_multiplier,
        );
    }

    if config.order.enabled {
        info!(
            "Order management: ENABLED (reprice_threshold={}bps, pending_timeout={}s, max_live_age={}s)",
            config.order.reprice_threshold_bps,
            config.order.pending_timeout_secs,
            config.order.max_live_age_secs,
        );
    } else {
        warn!(
            "TEST/OBSERVE-ONLY MODE: order.enabled=false; this process will never place orders"
        );
    }

    // Fetch symbol info from API (tick_size/lot_size auto-detection).
    // If we cannot determine these automatically, we refuse to run.
    if !config.symbol_info.enabled {
        anyhow::bail!("symbol_info.enabled must be true to auto-detect tick_size/lot_size");
    }

    let mut symbol_infos: HashMap<String, Arc<SharedSymbolInfo>> = HashMap::new();
    let client = StandXClient::new();
    for symbol in &config.symbols {
        let info = client.query_symbol_info(symbol).await.map_err(|e| {
            anyhow::anyhow!("[{}] Failed to fetch symbol info: {}", symbol, e)
        })?;

        let tick_size = info.tick_size();
        let lot_size = info.lot_size();

        // Validate fetched values - reject if invalid
        let tick_valid = tick_size.is_finite() && tick_size > 1e-12 && tick_size < 1e12;
        let lot_valid = lot_size.is_finite() && lot_size > 1e-12 && lot_size < 1e12;

        if !tick_valid || !lot_valid {
            anyhow::bail!(
                "[{}] API returned invalid values (tick_size={}, lot_size={})",
                symbol,
                tick_size,
                lot_size
            );
        }

        info!(
            "[{}] Fetched tick_size={} ({}dp), lot_size={} ({}dp) from API",
            symbol, tick_size, info.price_tick_decimals,
            lot_size, info.qty_tick_decimals
        );
        symbol_infos.insert(symbol.clone(), Arc::new(SharedSymbolInfo::from_api_response(&info)));
    }

    // Create SharedAlpha for Binance integration (lock-free reads from hot path)
    let shared_binance_alpha: Option<Arc<SharedAlpha>> = if config.strategy.alpha_source == "binance" {
        Some(Arc::new(SharedAlpha::new()))
    } else {
        None
    };

    // Start Binance alpha poller if configured
    let mut binance_alpha_handle: Option<BinanceAlphaPollerHandle> = None;
    if let Some(ref shared_alpha) = shared_binance_alpha {
        // Use btcusdt as the Binance symbol for alpha (BTC is the most liquid market)
        let binance_symbol = "btcusdt";
        let window_size = 300; // 30s window at 100ms updates
        let looking_depth = config.strategy.looking_depth;

        info!(
            "Starting Binance alpha poller (symbol={}, window={}s, depth={}%, stale_threshold={}ms)",
            binance_symbol,
            window_size / 10, // 300 samples @ 100ms = 30s
            looking_depth * 100.0,
            config.strategy.binance_stale_ms
        );

        binance_alpha_handle = Some(start_binance_alpha_poller(
            binance_symbol,
            window_size,
            looking_depth,
            Arc::clone(shared_alpha),
        ));
    } else {
        info!("Alpha source: StandX (Binance alpha disabled)");
    }

    // Create SharedBbo for Binance BBO integration (lock-free reads from hot path)
    let shared_binance_bbo: Option<Arc<SharedBbo>> = if config.strategy.alpha_source == "binance" {
        Some(Arc::new(SharedBbo::new()))
    } else {
        None
    };

    // Start Binance BBO poller if configured
    let mut binance_bbo_handle: Option<BinanceBboPollerHandle> = None;
    if let Some(ref shared_bbo) = shared_binance_bbo {
        let binance_symbol = "btcusdt";
        info!("Starting Binance BBO poller (symbol={})", binance_symbol);
        binance_bbo_handle = Some(start_binance_bbo_poller(
            binance_symbol,
            Arc::clone(shared_bbo),
        ));
    }

    // Create application with symbol info and Binance alpha/BBO
    let mut app = App::new(config.clone(), symbol_infos, shared_binance_alpha, shared_binance_bbo);

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

    if config.order.enabled {
        let auth = shared_auth
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("order.enabled requires valid credentials"))?;
        if !auth.lock().await.is_authenticated() {
            anyhow::bail!("order.enabled requires successful authentication");
        }
    }

    let (reconciliation_tx, reconciliation_rx) =
        mpsc::unbounded_channel::<ReconciliationRequest>();
    let (reconciliation_result_tx, mut reconciliation_result_rx) =
        mpsc::channel::<ReconciliationResult>(8);
    if config.order.enabled {
        let auth = Arc::clone(shared_auth.as_ref().expect("validated above"));
        tokio::spawn(run_reconciliation_worker(
            auth,
            reconciliation_rx,
            reconciliation_result_tx,
        ));
        app.set_reconciliation_tx(reconciliation_tx);
    } else {
        drop(reconciliation_rx);
        drop(reconciliation_result_tx);
    }

    // Ensure exchange leverage matches config for all symbols before trading
    // CRITICAL: If leverage cannot be verified/set, refuse to trade to prevent wrong-leverage risk
    let target_leverage = config.strategy.leverage as i32;
    if config.order.enabled {
        if let Some(ref auth) = shared_auth {
            for symbol in &config.symbols {
                let mut auth_guard = auth.lock().await;
                match auth_guard.query_leverage(symbol).await {
                    Ok(current) => {
                        if current != target_leverage {
                            info!("[{}] Leverage is {}x, changing to {}x...", symbol, current, target_leverage);
                            match auth_guard.set_leverage(symbol, target_leverage).await {
                                Ok(()) => {
                                    info!("[{}] Leverage set to {}x", symbol, target_leverage);
                                }
                                Err(e) => {
                                    error!("[{}] FATAL: Failed to set leverage to {}x: {} - refusing to trade", symbol, target_leverage, e);
                                    anyhow::bail!("[{}] Cannot set leverage to {}x: {}", symbol, target_leverage, e);
                                }
                            }
                        } else {
                            info!("[{}] Leverage verified at {}x", symbol, target_leverage);
                        }
                    }
                    Err(e) => {
                        error!("[{}] FATAL: Failed to query leverage: {} - refusing to trade", symbol, e);
                        anyhow::bail!("[{}] Cannot verify leverage: {}", symbol, e);
                    }
                }
            }
        }
    }

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
    let (executor_fault_tx, mut executor_fault_rx) = mpsc::channel::<String>(32);
    let (order_client, executor_handle_opt): (Option<Arc<OrderWsClient>>, Option<tokio::task::JoinHandle<()>>) = if config.order.enabled {
        if let Some(ref auth) = shared_auth {
            // Check if auth succeeded
            let auth_ok = {
                let auth_guard = auth.lock().await;
                auth_guard.is_authenticated()
            };

            if !auth_ok {
                error!("Order management disabled (authentication failed)");
                (None, None)
            } else {
                // Cancel all existing account orders before starting and prove
                // that cleanup completed before the order socket is created.
                if !cancel_all_and_verify(auth, "startup", 3).await {
                    anyhow::bail!(
                        "startup reconciliation could not verify zero open account orders"
                    );
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
                let executor_fault_tx = executor_fault_tx.clone();

                // Clone symbol_infos for executor to read dynamic tick_size/lot_size
                let executor_symbol_infos = app.symbol_infos().clone();
                // Fallback values from config if symbol not in map
                let fallback_tick_size = app.config.strategy.tick_size;
                let fallback_lot_size = app.config.strategy.lot_size;

                // Spawn order executor task (store handle for crash detection)
                let executor_handle = tokio::spawn(async move {
                    let mut order_rx = order_rx;
                    info!("Order executor task started");
                    while let Some((symbol, decision)) = order_rx.recv().await {
                        match decision {
                            OrderDecision::Send { side, level, price, qty, cl_ord_id } => {
                                // Get current tick_size/lot_size from SharedSymbolInfo (dynamic)
                                let (tick_size, lot_size) = executor_symbol_infos
                                    .get(symbol.as_ref())
                                    .map(|info| (info.tick_size(), info.lot_size()))
                                    .unwrap_or((fallback_tick_size, fallback_lot_size));

                                let req = match side {
                                    // Dereference Arc<str> to &str for Into<String>
                                    Side::Buy => NewOrderRequest::post_only_buy_with_precision(&*symbol, price, qty, tick_size, lot_size)
                                        .with_client_id(&cl_ord_id),
                                    Side::Sell => NewOrderRequest::post_only_sell_with_precision(&*symbol, price, qty, tick_size, lot_size)
                                        .with_client_id(&cl_ord_id),
                                };
                                debug!("[{}] Sending {} L{} order: {} @ {:.2}", symbol, side, level, cl_ord_id, price);
                                if let Err(e) = executor_client.place_order(req).await {
                                    warn!("[{}] Failed to place order: {}", symbol, e);
                                    let _ = executor_fault_tx
                                        .send(format!("place {} {} failed: {}", symbol, cl_ord_id, e))
                                        .await;
                                }
                            }
                            OrderDecision::Cancel { cl_ord_id } => {
                                debug!("[{}] Canceling order: {}", symbol, cl_ord_id);
                                if let Err(e) = executor_client.cancel_order_by_client_id(&cl_ord_id).await {
                                    warn!("[{}] Failed to cancel order: {}", symbol, e);
                                    let _ = executor_fault_tx
                                        .send(format!("cancel {} {} failed: {}", symbol, cl_ord_id, e))
                                        .await;
                                }
                            }
                            OrderDecision::CancelAndReplace { cancel_id, level: _, new_price, qty: _ } => {
                                debug!("[{}] Cancel and replace: {} -> {:.2}", symbol, cancel_id, new_price);
                                // Cancel only - new order will be placed on next quote cycle
                                if let Err(e) = executor_client.cancel_order_by_client_id(&cancel_id).await {
                                    warn!("[{}] Failed to cancel order for replacement: {}", symbol, e);
                                    let _ = executor_fault_tx
                                        .send(format!("replace-cancel {} {} failed: {}", symbol, cancel_id, e))
                                        .await;
                                }
                            }
                            OrderDecision::NoAction => {}
                        }
                    }
                    info!("Order executor task stopped");
                });

                (Some(ws_client), Some(executor_handle))
            }
        } else {
            warn!("Order management disabled (no credentials)");
            (None, None)
        }
    } else {
        (None, None)
    };
    let mut executor_handle: Option<tokio::task::JoinHandle<()>> = executor_handle_opt;

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
            let shared_equity = app.shared_equity();
            let tracker = WalletTracker::new(Arc::clone(auth), tracker_config, position, trading_stats, shared_equity);
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

    // Start open orders checker if order management is enabled
    // This detects when exchange has no orders but internal state thinks we do,
    // allowing immediate new order placement instead of waiting for timeout.
    let mut order_checker_handles: Vec<OpenOrdersCheckerHandle> = Vec::new();
    let mut clear_orders_rx: Option<mpsc::Receiver<ClearOrdersSignal>> = None;
    let mut open_orders_snapshot_rx: Option<mpsc::Receiver<OpenOrdersSnapshot>> = None;
    if config.order.enabled {
        if let Some(ref auth) = shared_auth {
            // Create channel for clear signals (small buffer, we only need latest)
            let (clear_tx, clear_rx) = mpsc::channel::<ClearOrdersSignal>(10);
            clear_orders_rx = Some(clear_rx);
            // REST identities are consumed only by the one-second cold path.
            let (snapshot_tx, snapshot_rx) = mpsc::channel::<OpenOrdersSnapshot>(100);
            open_orders_snapshot_rx = Some(snapshot_rx);

            // Start checker for each symbol
            for symbol in &config.symbols {
                let checker_config = OpenOrdersCheckerConfig {
                    interval: Duration::from_secs(3), // Poll every 3 seconds
                    symbol: symbol.clone(),
                    debounce_count: 2, // 2 consecutive zero polls = 6 seconds
                    max_order_age_secs: config.order.max_live_age_secs * 2, // 2x max live age
                    expected_order_levels: config.strategy.order_levels, // Match strategy levels
                };
                let checker = OpenOrdersChecker::new(
                    Arc::clone(auth),
                    checker_config,
                    clear_tx.clone(),
                    snapshot_tx.clone(),
                );
                order_checker_handles.push(checker.start());
            }
            info!(
                "Open orders checker started (interval: 3s, debounce: 2, max_age: {}s, levels: {})",
                config.order.max_live_age_secs * 2,
                config.strategy.order_levels
            );
        }
    }

    // Start symbol info poller for tick size change detection
    let mut symbol_info_handles: Vec<SymbolInfoPollerHandle> = Vec::new();
    let mut tick_size_changed_rx: Option<mpsc::Receiver<TickSizeChangedSignal>> = None;
    if config.symbol_info.enabled {
        // Create channel for tick size change signals
        let (tx, rx) = mpsc::channel::<TickSizeChangedSignal>(10);
        tick_size_changed_rx = Some(rx);

        // Start poller for each symbol
        for (symbol, shared_info) in app.symbol_infos() {
            let poller_config = SymbolInfoPollerConfig {
                interval: Duration::from_secs(config.symbol_info.poll_interval_secs),
                symbol: symbol.clone(),
            };
            let poller = SymbolInfoPoller::new(
                Arc::clone(shared_info),
                poller_config,
                tx.clone(),
            );
            symbol_info_handles.push(poller.start());
        }
        info!(
            "Symbol info poller started (interval: {}s)",
            config.symbol_info.poll_interval_secs
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
                if app.process_event(event) {
                    app.begin_account_reconciliation("market data disconnected; warm-up reset");
                }
            }

            Some(result) = reconciliation_result_rx.recv(), if config.order.enabled => {
                if result.generation == app.reconciliation_generation() {
                    info!(
                        "Account reconciliation verified after {} attempt(s): {}",
                        result.attempts, result.reason
                    );
                    if let Some(ref client) = order_client {
                        client.clear_pending_tracking().await;
                    }
                    app.finish_account_reconciliation();
                } else {
                    debug!(
                        "Ignoring reconciliation generation {} because generation {} is pending",
                        result.generation,
                        app.reconciliation_generation()
                    );
                }
            }

            Some(fault) = executor_fault_rx.recv(), if config.order.enabled => {
                error!("Ambiguous order transport failure: {}", fault);
                for manager in app.order_managers_mut().values_mut() {
                    manager.trigger_safety_pause("ambiguous order transport failure");
                }
                app.begin_account_reconciliation(fault);
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
                                let matched = app
                                    .get_order_manager_mut(symbol)
                                    .is_some_and(|manager| {
                                        manager.on_order_accepted(&cl_ord_id, order_id)
                                    });
                                if !matched {
                                    app.begin_account_reconciliation(format!(
                                        "accepted order could not be correlated: {}",
                                        cl_ord_id
                                    ));
                                }
                            }
                            None => {
                                warn!("Failed to extract symbol from cl_ord_id='{}', order_id={}", cl_ord_id, order_id);
                                app.begin_account_reconciliation(format!(
                                    "accepted order had invalid client ID: {}",
                                    cl_ord_id
                                ));
                            }
                        }
                    }
                    OrderEvent::OrderSubmissionAcknowledged => {
                        debug!(
                            "Order submission acknowledged without identity; awaiting REST confirmation"
                        );
                    }
                    OrderEvent::OrderRejected { cl_ord_id, reason } => {
                        // O(1) lookup: extract symbol from cl_ord_id
                        if let Some(symbol) = QuoteOrderManager::extract_symbol_from_cl_ord_id(&cl_ord_id) {
                            if let Some(manager) = app.get_order_manager_mut(symbol) {
                                manager.on_order_rejected(&cl_ord_id, &reason);
                            }
                        }
                    }
                    OrderEvent::OrderFilled {
                        order_id,
                        cl_ord_id,
                        status,
                        fill_qty,
                        fill_price,
                    } => {
                        info!(
                            "Fill received: order_id={}, cl_ord_id={:?}, status={:?}, qty={}, price={}",
                            order_id, cl_ord_id, status, fill_qty, fill_price
                        );
                        let fill_quantity = fill_qty.parse::<f64>().unwrap_or(0.0);
                        let normalized_status = status.as_deref().unwrap_or("");
                        let fully_filled = normalized_status == "filled";
                        let partial = matches!(
                            normalized_status,
                            "partial" | "partially_filled" | "partially-filled"
                        );

                        if !fill_quantity.is_finite() || fill_quantity <= 0.0 {
                            for manager in app.order_managers_mut().values_mut() {
                                manager.require_position_refresh_after_fill();
                            }
                            app.begin_account_reconciliation(
                                "fill response had an invalid quantity",
                            );
                            continue;
                        }

                        if !fully_filled && !partial {
                            for manager in app.order_managers_mut().values_mut() {
                                manager.require_position_refresh_after_fill();
                            }
                            app.begin_account_reconciliation(
                                "fill response lacked an unambiguous status",
                            );
                            continue;
                        }

                        let mut matched = false;
                        if let Some(ref client_id) = cl_ord_id {
                            if let Some(symbol) = QuoteOrderManager::extract_symbol_from_cl_ord_id(client_id) {
                                if let Some(manager) = app.get_order_manager_mut(symbol) {
                                    matched = manager.on_order_fill(
                                        order_id,
                                        Some(client_id),
                                        fill_quantity,
                                        fully_filled,
                                    );
                                }
                            }
                        }
                        if !matched {
                            for manager in app.order_managers_mut().values_mut() {
                                if manager.on_order_fill(
                                    order_id,
                                    cl_ord_id.as_deref(),
                                    fill_quantity,
                                    fully_filled,
                                ) {
                                    matched = true;
                                    break;
                                }
                            }
                        }
                        if !matched {
                            for manager in app.order_managers_mut().values_mut() {
                                manager.require_position_refresh_after_fill();
                            }
                            app.begin_account_reconciliation(
                                "fill could not be matched to a tracked order",
                            );
                        } else if partial {
                            app.begin_account_reconciliation(
                                "partial fill requires verified remainder cleanup",
                            );
                        }
                    }
                    OrderEvent::OrderCanceled { order_id, cl_ord_id } => {
                        // First try matching by cl_ord_id (works for orders that were never accepted)
                        let mut matched_symbol: Option<String> = None;
                        if let Some(ref cl_ord_id) = cl_ord_id {
                            if let Some(symbol) = QuoteOrderManager::extract_symbol_from_cl_ord_id(cl_ord_id) {
                                if let Some(manager) = app.get_order_manager_mut(symbol) {
                                    if manager.on_order_canceled_by_cl_ord_id(cl_ord_id) {
                                        matched_symbol = Some(symbol.to_string());
                                    }
                                }
                            }
                        }
                        // Fallback: scan all managers by order_id
                        if matched_symbol.is_none() {
                            for (sym, manager) in app.order_managers_mut() {
                                if manager.on_order_canceled(order_id) {
                                    matched_symbol = Some(sym.clone());
                                    break;
                                }
                            }
                        }

                        // Check for pending replacement orders and execute immediately
                        let mut pending_dispatch_failed = false;
                        if let Some(symbol) = matched_symbol {
                            let order_tx = app.order_tx.clone();
                            if let Some(manager) = app.get_order_manager_mut(&symbol) {
                                use std::time::{SystemTime, UNIX_EPOCH};
                                let now_ns = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_nanos() as i64;
                                let pending_decisions = manager.check_pending_orders(now_ns);
                                for decision in pending_decisions {
                                    let symbol_arc: Arc<str> = Arc::from(symbol.as_str());
                                    if let Some(ref order_tx) = order_tx {
                                        if let Err(error) = order_tx.send((symbol_arc, decision)).await {
                                            let (_, failed_decision) = error.0;
                                            manager.on_dispatch_failed(&failed_decision);
                                            manager.trigger_safety_pause(
                                                "replacement order could not reach executor",
                                            );
                                            pending_dispatch_failed = true;
                                            error!(
                                                "[{}] Replacement order could not reach executor",
                                                symbol
                                            );
                                        }
                                    } else {
                                        manager.on_dispatch_failed(&decision);
                                        manager.trigger_safety_pause(
                                            "replacement order has no executor",
                                        );
                                        pending_dispatch_failed = true;
                                        error!("[{}] Replacement order has no executor", symbol);
                                    }
                                }
                            }
                        }
                        if pending_dispatch_failed {
                            app.begin_account_reconciliation(
                                "replacement order could not reach executor",
                            );
                        }
                    }
                    OrderEvent::CancelSubmissionAcknowledged => {
                        // Without request or order identity, a successful response
                        // proves submission only. Account-wide REST cleanup is the
                        // safe way to establish the final cancel state.
                        app.begin_account_reconciliation(
                            "cancel submission acknowledged without identity",
                        );
                    }
                    OrderEvent::CancelFailed { order_id, reason } => {
                        // Cancel failed - order is still live, revert state
                        for manager in app.order_managers_mut().values_mut() {
                            manager.on_cancel_failed(order_id, &reason);
                        }
                        let requires_reconciliation = order_id == 0
                            || app
                                .order_managers_mut()
                                .values()
                                .any(|manager| {
                                    manager.has_pause_reason(PauseReason::Reconciliation)
                                });
                        if requires_reconciliation {
                            app.begin_account_reconciliation(
                                "cancel rejection requires verified account reconciliation",
                            );
                        }
                    }
                    OrderEvent::Connected => {
                        info!("Order WebSocket connected");
                        app.order_ws_connected = true;
                    }
                    OrderEvent::Authenticated => {
                        info!("Order WebSocket authenticated");
                        app.order_ws_authenticated = true;
                        // Clear only the order-transport reason. Warm-up, risk data,
                        // and reconciliation independently keep trading paused.
                        for manager in app.order_managers_mut().values_mut() {
                            manager.resume();
                        }
                    }
                    OrderEvent::Disconnected(reason) => {
                        warn!("Order WebSocket disconnected: {}", reason);
                        app.order_ws_connected = false;
                        app.order_ws_authenticated = false;

                        // Pause immediately. Do not clear local state until REST verifies
                        // that account-wide cleanup completed.
                        for manager in app.order_managers_mut().values_mut() {
                            manager.pause();
                        }
                        app.begin_account_reconciliation("order WebSocket disconnected");
                    }
                    OrderEvent::Reconnecting { attempt, delay_secs } => {
                        info!("Order WebSocket reconnecting in {}s (attempt {})", delay_secs, attempt);
                    }
                    OrderEvent::MaxRetriesExceeded => {
                        error!("Order WebSocket max retries exceeded, initiating shutdown");
                        app.shutdown_order_managers();
                        if let Some(auth) = &shared_auth {
                            cancel_all_and_verify(auth, "order WebSocket failure", 5).await;
                            log_account_positions(auth, "order WebSocket failure").await;
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
                    OrderEvent::AmbiguousResponse { request_id, code, reason } => {
                        error!(
                            "Ambiguous order response: request_id={:?}, code={:?}, reason={}",
                            request_id, code, reason
                        );
                        app.begin_account_reconciliation(reason);
                    }
                }
            }

            // Detect order executor crash (if it exits unexpectedly, trigger shutdown)
            result = async {
                match executor_handle.as_mut() {
                    Some(handle) => Some(handle.await),
                    None => std::future::pending().await,
                }
            } => {
                if let Some(result) = result {
                    match result {
                        Ok(()) => {
                            error!("FATAL: Order executor task exited unexpectedly - initiating shutdown");
                        }
                        Err(e) => {
                            error!("FATAL: Order executor task panicked: {} - initiating shutdown", e);
                        }
                    }
                    app.shutdown_order_managers();
                    if let Some(auth) = &shared_auth {
                        cancel_all_and_verify(auth, "order executor failure", 5).await;
                        log_account_positions(auth, "order executor failure").await;
                    }
                    break;
                }
            }

            // Handle shutdown signal
            _ = signal::ctrl_c() => {
                info!("Received shutdown signal");

                // Stop local order creation first, then perform account-wide
                // cancellation even if local tracking currently has no orders.
                let orders_to_cancel = app.shutdown_order_managers();
                info!("Local state tracks {} order(s) at shutdown", orders_to_cancel.len());
                if config.order.enabled {
                    if let Some(auth) = &shared_auth {
                        cancel_all_and_verify(auth, "shutdown", 5).await;
                        log_account_positions(auth, "final").await;
                    }
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

            // Handle tick size change signal (CRITICAL: must stop trading, reset, restart)
            Some(signal) = async {
                tick_size_changed_rx.as_mut().unwrap().recv().await
            }, if tick_size_changed_rx.is_some() => {
                warn!(
                    "[{}] TICK SIZE CHANGED: {} -> {} (lot_size: {} -> {}) - STOPPING TRADING",
                    signal.symbol,
                    signal.old_tick_size, signal.new_tick_size,
                    signal.old_lot_size, signal.new_lot_size
                );

                app.begin_account_reconciliation(format!(
                    "tick/lot size changed for {}",
                    signal.symbol
                ));

                // Reset strategy (clears rolling stats, re-enters warmup)
                app.reset_strategy(&signal.symbol);
                if let Some(manager) = app.get_order_manager_mut(&signal.symbol) {
                    manager.set_pause_reason(PauseReason::MarketData);
                }

                info!(
                    "[{}] Tick/lot update applied: tick_size={}, lot_size={} - trading remains paused through reconciliation and warmup",
                    signal.symbol, signal.new_tick_size, signal.new_lot_size
                );
            }

            // Periodic tasks: timeout checking and stats logging (runs every 1 second)
            _ = periodic_interval.tick() => {
                if app.update_risk_pauses() {
                    app.begin_account_reconciliation("position or equity data became stale");
                }

                // Bind successful, identity-less submission ACKs to exchange
                // order IDs from authenticated REST. This is intentionally kept
                // out of the quote hot path.
                let mut rest_identity_fault = None;
                if let Some(ref mut rx) = open_orders_snapshot_rx {
                    while let Ok(snapshot) = rx.try_recv() {
                        match app.apply_open_orders_snapshot(&snapshot) {
                            Ok(confirmed) if confirmed > 0 => {
                                if let Some(ref client) = order_client {
                                    let client_ids: Vec<&str> = snapshot
                                        .orders
                                        .iter()
                                        .filter_map(|order| order.cl_ord_id.as_deref())
                                        .collect();
                                    client.try_clear_rest_confirmed_requests(&client_ids);
                                }
                                debug!(
                                    "[{}] REST confirmed {} tracked open order(s)",
                                    snapshot.symbol, confirmed
                                );
                            }
                            Ok(_) => {}
                            Err(reason) => {
                                rest_identity_fault = Some(reason);
                                break;
                            }
                        }
                    }
                }
                if let Some(reason) = rest_identity_fault {
                    error!("REST order identity check failed: {}", reason);
                    app.begin_account_reconciliation(reason);
                }

                // Check for clear order signals from OpenOrdersChecker (non-blocking)
                // This is O(1) try_recv - zero latency impact on hot path
                if let Some(ref mut rx) = clear_orders_rx {
                    while let Ok(signal) = rx.try_recv() {
                        let manager_has_orders = app
                            .get_order_manager_mut(&signal.symbol)
                            .map(|manager| !manager.get_all_live_order_ids().is_empty())
                            .unwrap_or(false);
                        if signal.reason.starts_with("0 orders") && !manager_has_orders {
                            continue;
                        }
                        if signal.pause_trading {
                            // CRITICAL: >2 orders detected - this is a safety limit violation
                            error!(
                                "[{}] SAFETY LIMIT: {} - PAUSING TRADING",
                                signal.symbol, signal.reason
                            );
                        } else {
                            info!(
                                "[{}] Stale state detected: {} - canceling all orders",
                                signal.symbol, signal.reason
                            );
                        }

                        // Pause first. Clear local state only after REST verifies that
                        // account-wide cleanup completed.
                        if let Some(manager) = app.get_order_manager_mut(&signal.symbol) {
                            if signal.pause_trading {
                                manager.trigger_safety_pause(&signal.reason);
                            }
                        }
                        app.begin_account_reconciliation(format!(
                            "open-order checker for {}: {}",
                            signal.symbol, signal.reason
                        ));
                    }
                }

                // Check pending order timeouts (independent of market data)
                app.check_order_timeouts();

                // Proactive JWT refresh: force WS reconnect before token expires.
                // The ed25519 keypair is stable across refreshes, so re-authentication
                // after reconnect will work seamlessly with the same signing key.
                // Check every periodic tick (1s) but only act when token is near expiry.
                if let Some(ref auth) = shared_auth {
                    if let Ok(auth_guard) = auth.try_lock() {
                        let remaining = auth_guard.token_remaining_secs();
                        // Reconnect when < 2 hours remaining (well before 1-hour HTTP refresh threshold)
                        if remaining > 0 && remaining < 7200 {
                            drop(auth_guard);
                            if let Some(ref oc) = order_client {
                                if oc.is_connected() {
                                    warn!(
                                        "JWT token expires in {}s, forcing order WS reconnect for re-authentication",
                                        remaining
                                    );
                                    // Pause order managers while reconnecting
                                    for manager in app.order_managers_mut().values_mut() {
                                        manager.pause();
                                    }
                                    app.begin_account_reconciliation("JWT refresh reconnect");
                                    let oc_clone = Arc::clone(oc);
                                    tokio::spawn(async move {
                                        oc_clone.force_reconnect().await;
                                    });
                                }
                            }
                        }
                    }
                }

                // Check circuit breaker auto-recovery
                app.check_circuit_breaker_recovery();

                // Check safety pause auto-recovery (separate from circuit breaker)
                app.check_safety_pause_recovery(config.order.safety_pause_recovery_secs);

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

    // Stop open orders checker
    if !order_checker_handles.is_empty() {
        info!("Stopping {} open orders checker(s)...", order_checker_handles.len());
        for handle in order_checker_handles {
            handle.stop();
        }
    }

    // Stop symbol info pollers
    if !symbol_info_handles.is_empty() {
        info!("Stopping {} symbol info poller(s)...", symbol_info_handles.len());
        for handle in symbol_info_handles {
            handle.stop();
        }
    }

    // Stop Binance alpha poller
    if let Some(handle) = binance_alpha_handle {
        info!("Stopping Binance alpha poller...");
        handle.stop();
    }

    // Stop Binance BBO poller
    if let Some(handle) = binance_bbo_handle {
        info!("Stopping Binance BBO poller...");
        handle.stop();
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
