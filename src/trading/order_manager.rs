//! Low-latency order manager for market making.
//!
//! This module provides synchronous order decision-making with
//! fire-and-forget async execution for minimal latency impact.
//!
//! # Architecture
//!
//! ```text
//! OrderManager.on_quote()  ──►  Vec<OrderDecision>  ──►  mpsc channel
//!       (SYNC, <10µs)                                      (async executor)
//! ```
//!
//! - Hot path (`on_quote`) is synchronous and lock-free
//! - Order execution happens asynchronously via channel
//! - State machine tracks orders: Pending → Live → Canceling
//! - Supports up to 2 order levels per side (4 orders total)

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::{debug, error, info, warn};

use crate::strategy::Quote;
use crate::trading::{SharedEquity, SharedPosition};

/// Maximum number of order levels supported (2 bids + 2 asks).
/// Must match MAX_ORDER_LEVELS in strategy/quotes.rs
pub const MAX_ORDER_LEVELS: usize = 2;

/// Pause reason bitfield values.
/// Multiple pause reasons can be active simultaneously; trading resumes
/// only when ALL reasons have been cleared.
/// Independent reasons that can block order generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PauseReason {
    OrderWebSocket = 1,
    CircuitBreaker = 2,
    Safety = 4,
    MarketData = 8,
    RiskData = 16,
    Reconciliation = 32,
}

/// Order side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "buy"),
            Side::Sell => write!(f, "sell"),
        }
    }
}

/// Order state in the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    /// Order sent, awaiting confirmation from exchange.
    Pending,
    /// Order confirmed and live on the exchange.
    Live,
    /// Cancel request sent, awaiting confirmation.
    Canceling,
}

/// Tracked live order (one per level per side per symbol).
#[derive(Debug, Clone)]
pub struct LiveOrder {
    /// Client order ID (used for cancellation).
    pub cl_ord_id: String,
    /// Exchange order ID (set on acceptance).
    pub order_id: Option<i64>,
    /// Order side.
    pub side: Side,
    /// Order level (0 = inner, 1 = outer).
    pub level: usize,
    /// Order price.
    pub price: f64,
    /// Order quantity.
    pub quantity: f64,
    /// Current state in the state machine.
    pub state: OrderState,
    /// Timestamp when order was sent (nanoseconds).
    pub sent_at_ns: i64,
    /// Consecutive cancel failure count (for detecting stuck orders).
    pub cancel_fail_count: u32,
}

/// Order decision from the hot path.
#[derive(Debug, Clone)]
pub enum OrderDecision {
    /// No action needed.
    NoAction,
    /// Send a new order.
    Send {
        side: Side,
        level: usize,
        price: f64,
        qty: f64,
        cl_ord_id: String,
    },
    /// Cancel an existing order.
    Cancel {
        cl_ord_id: String,
    },
    /// Cancel existing order and prepare for replacement.
    /// Note: New order will be sent on next quote cycle after cancel confirms.
    CancelAndReplace {
        cancel_id: String,
        level: usize,
        new_price: f64,
        qty: f64,
    },
}

/// Reusable decision buffer used by the latency-critical quote path.
pub type OrderDecisions = Vec<OrderDecision>;

/// Order manager statistics.
#[derive(Debug, Default, Clone)]
pub struct OrderManagerStats {
    /// Total orders sent.
    pub orders_sent: u64,
    /// Orders accepted by exchange.
    pub orders_accepted: u64,
    /// Orders canceled.
    pub orders_canceled: u64,
    /// Orders rejected.
    pub rejections: u64,
    /// Reprice events (cancel + replace).
    pub reprices: u64,
    /// Timeouts (pending orders that timed out).
    pub timeouts: u64,
}

/// Order manager configuration.
#[derive(Debug, Clone)]
pub struct OrderManagerConfig {
    /// Symbol being managed.
    pub symbol: String,
    /// Reprice threshold in basis points (default: 1.0).
    pub reprice_threshold_bps: f64,
    /// Order timeout in nanoseconds (applies to Pending and Canceling states).
    pub pending_timeout_ns: u64,
    /// Maximum age for Live orders in nanoseconds before forcing refresh.
    /// Even if price is within reprice threshold, orders older than this are repriced.
    /// Set to 0 to disable (no max age for Live orders).
    pub max_live_age_ns: u64,
    /// Tick size for price snapping.
    pub tick_size: f64,
    /// Lot size for quantity.
    pub lot_size: f64,
    pub min_order_qty: f64,
    pub min_order_qty_dollar: f64,
    /// Enable debug logging for order state tracking.
    pub debug: bool,
    /// Maximum consecutive rejections before pausing trading (circuit breaker).
    /// Set to 0 to disable.
    pub circuit_breaker_rejections: u32,
    /// Number of order levels per side (1 or 2).
    pub num_levels: usize,
    /// Optional hard position/exposure cap in dollars.
    pub absolute_max_position_dollar: Option<f64>,
    /// Measure the hard cap relative to the first fresh position.
    pub position_limit_from_start: bool,
    /// Optional hard notional cap per individual order.
    pub max_order_qty_dollar: Option<f64>,
}

impl Default for OrderManagerConfig {
    fn default() -> Self {
        Self {
            symbol: "TEST-USD".to_string(),
            reprice_threshold_bps: 1.0,
            pending_timeout_ns: 5_000_000_000, // 5 seconds
            max_live_age_ns: 60_000_000_000,   // 60 seconds max age for Live orders
            tick_size: 0.01,
            lot_size: 0.001,
            min_order_qty: 0.001,
            min_order_qty_dollar: 10.0,
            debug: false,
            circuit_breaker_rejections: 5,     // Pause after 5 consecutive rejections
            num_levels: 1,                     // Default to single level
            absolute_max_position_dollar: None,
            position_limit_from_start: false,
            max_order_qty_dollar: None,
        }
    }
}

/// Timeout check interval in nanoseconds (1 second).
/// This limits how often we check for timed out orders to minimize hot path latency.
const TIMEOUT_CHECK_INTERVAL_NS: i64 = 1_000_000_000;

/// Shared pause state and the barrier between new-order writes and cleanup.
#[derive(Default)]
pub struct ExecutionState {
    pause_reasons: AtomicU8,
    shutdown: AtomicBool,
    generation: AtomicU64,
    pub submission: tokio::sync::Mutex<()>,
}

impl ExecutionState {
    pub fn generation(&self) -> u64 { self.generation.load(Ordering::Acquire) }
    pub fn permits(&self, generation: u64) -> bool {
        !self.shutdown.load(Ordering::Acquire)
            && self.pause_reasons.load(Ordering::Acquire) == 0
            && generation == self.generation()
    }
}

/// Low-latency order manager for market making.
///
/// Provides synchronous order decision-making in the hot path with
/// fire-and-forget async execution.
///
/// Supports up to MAX_ORDER_LEVELS (2) per side for multi-level market making.
pub struct OrderManager {
    /// Configuration.
    config: OrderManagerConfig,

    /// Current bid orders per level (buy).
    bid_orders: [Option<LiveOrder>; MAX_ORDER_LEVELS],

    /// Current ask orders per level (sell).
    ask_orders: [Option<LiveOrder>; MAX_ORDER_LEVELS],

    /// Shared position from poller (lock-free reads).
    position: Arc<SharedPosition>,

    /// Shared equity for max_position_dollar (lock-free reads, ~1ns).
    shared_equity: Arc<SharedEquity>,

    /// Client order ID counter.
    cl_ord_id_counter: AtomicU64,

    /// Session prefix for client order IDs.
    session_prefix: String,
    retired_generation: u64,

    /// Statistics.
    stats: OrderManagerStats,

    /// Shutdown flag.
    execution: Arc<ExecutionState>,

    /// Last time we checked for timeouts (throttles hot path).
    last_timeout_check_ns: i64,

    /// Consecutive rejection count for circuit breaker.
    consecutive_rejections: u32,

    /// Timestamp when circuit breaker was triggered (Unix millis, 0 if not triggered).
    circuit_breaker_triggered_at_ms: u64,

    /// Timestamp when safety pause was triggered (Unix millis, 0 if not triggered).
    /// Safety pause is triggered when orders exceed expected count (duplicate/stuck state).
    safety_pause_triggered_at_ms: u64,

    /// Timestamp when last order was sent (Unix millis, 0 if never).
    last_order_sent_at_ms: u64,

    /// Position baseline used by guarded canaries.
    position_baseline: Option<f64>,

    /// A correlated fill blocks replacement orders until two later position
    /// polls have published, closing the fill-to-poller exposure window.
    resume_after_position_version: Option<u64>,
}

impl OrderManager {
    /// Create a new order manager.
    pub fn new(
        config: OrderManagerConfig,
        position: Arc<SharedPosition>,
        shared_equity: Arc<SharedEquity>,
    ) -> Self {
        // Generate session prefix including symbol for O(1) lookup by symbol in event handlers
        // Format: mm_<symbol>_<timestamp>
        let session_prefix = format!("mm_{}_{}", config.symbol, chrono::Utc::now().timestamp_millis());

        Self {
            config,
            bid_orders: [None, None],
            ask_orders: [None, None],
            position,
            shared_equity,
            cl_ord_id_counter: AtomicU64::new(1),
            session_prefix,
            retired_generation: 0,
            stats: OrderManagerStats::default(),
            execution: Arc::new(ExecutionState::default()),
            last_timeout_check_ns: 0,
            consecutive_rejections: 0,
            circuit_breaker_triggered_at_ms: 0,
            safety_pause_triggered_at_ms: 0,
            last_order_sent_at_ms: 0,
            position_baseline: None,
            resume_after_position_version: None,
        }
    }

    /// Get the configured number of levels.
    #[inline]
    pub fn num_levels(&self) -> usize {
        self.config.num_levels
    }

    /// Get the symbol being managed.
    pub fn symbol(&self) -> &str {
        &self.config.symbol
    }

    /// Get current statistics.
    pub fn stats(&self) -> &OrderManagerStats {
        &self.stats
    }

    /// Generate a unique client order ID with level.
    /// Format: mm_<symbol>_<timestamp>_<level>_<generation>_<seq>
    #[inline]
    pub fn generate_cl_ord_id(&self, level: usize) -> String {
        let seq = self.cl_ord_id_counter.fetch_add(1, Ordering::Relaxed);
        format!("{}_{}_{}_{}", self.session_prefix, level, self.execution.generation(), seq)
    }

    /// Responses for orders cleared by verified cleanup cannot recreate state.
    pub fn is_retired_client_id(&self, id: &str) -> bool {
        id.strip_prefix(&format!("{}_", self.session_prefix))
            .and_then(|suffix| suffix.split('_').nth(1))
            .and_then(|generation| generation.parse::<u64>().ok())
            .is_some_and(|generation| generation < self.retired_generation)
    }

    /// Extract symbol from a client order ID.
    /// Format: mm_<symbol>_<timestamp>_<level>_<generation>_<seq>
    /// Returns None if format doesn't match.
    #[inline]
    pub fn extract_symbol_from_cl_ord_id(cl_ord_id: &str) -> Option<&str> {
        // Format: mm_<symbol>_<timestamp>_<level>_<generation>_<seq>
        // Skip "mm_", find symbol between first and second "_" after "mm_"
        let rest = cl_ord_id.strip_prefix("mm_")?;
        // Find the first underscore (after symbol)
        let underscore_pos = rest.find('_')?;
        Some(&rest[..underscore_pos])
    }

    /// Check for timed out pending orders and return cancel decisions.
    ///
    /// This should be called on EVERY orderbook update, not just when quotes
    /// are generated. This ensures pending orders are cleaned up even when
    /// the strategy isn't producing quotes.
    ///
    /// To minimize hot path latency, this only performs the actual timeout
    /// check once per second (TIMEOUT_CHECK_INTERVAL_NS).
    #[inline]
    pub fn check_pending_timeouts(&mut self, current_time_ns: i64) -> Vec<OrderDecision> {
        // Fast path: skip check if not enough time has passed (single comparison)
        if current_time_ns - self.last_timeout_check_ns < TIMEOUT_CHECK_INTERVAL_NS {
            return vec![];
        }

        // Update last check time
        self.last_timeout_check_ns = current_time_ns;

        // Perform the actual timeout check
        self.check_timeouts(current_time_ns)
    }

    /// Check for timed out pending orders (no throttling).
    ///
    /// This is used by the background timeout checker task to ensure
    /// pending orders are cleaned up even when market data updates are sparse.
    #[inline]
    pub fn check_timeouts_now(&mut self, current_time_ns: i64) -> Vec<OrderDecision> {
        self.check_timeouts(current_time_ns)
    }

    /// Process a new quote and return order decisions.
    ///
    /// This is the HOT PATH - must complete in <10µs.
    /// Called synchronously in the main event loop.
    ///
    /// NOTE: Timeout checks are done separately via check_pending_timeouts()
    /// which is called on every orderbook update, not just when quotes are generated.
    #[inline]
    pub fn on_quote(&mut self, quote: &Quote, current_time_ns: i64) -> Vec<OrderDecision> {
        let mut decisions = Vec::with_capacity(self.config.num_levels * 4);
        self.on_quote_into(quote, current_time_ns, &mut decisions);
        decisions
    }

    /// Allocation-free quote processing for callers that retain the buffer.
    #[inline]
    pub fn on_quote_into(
        &mut self,
        quote: &Quote,
        current_time_ns: i64,
        decisions: &mut Vec<OrderDecision>,
    ) {
        decisions.clear();
        // Early exit if shutting down or paused (atomic, no latency)
        // Use Acquire ordering to ensure we see the Release store from shutdown()/pause()
        if self.execution.shutdown.load(Ordering::Acquire) || self.execution.pause_reasons.load(Ordering::Acquire) != 0 {
            return;
        }

        // Early exit if quote not valid for trading
        if !quote.valid_for_trading {
            return;
        }

        // Validate mid_price to prevent NaN/Inf from bypassing position limits
        // (NaN comparisons always return false, which would skip all limit checks)
        if !quote.mid_price.is_finite() || quote.mid_price <= 0.0
            || !quote.quantity.is_finite() || quote.quantity <= 0.0 {
            return;
        }

        let position = self.position.get();
        let limit = self.shared_equity.max_position_dollar();
        if !position.is_finite() || !limit.is_finite() || limit <= 0.0 {
            self.set_pause_reason(PauseReason::RiskData);
            self.cancel_exposure(decisions, current_time_ns);
            return;
        }
        let baseline = if self.config.position_limit_from_start {
            let Some(baseline) = self.position_baseline else { return; };
            baseline
        } else { 0.0 };
        for side in [Side::Buy, Side::Sell] {
            let sign = if side == Side::Buy { 1.0 } else { -1.0 };
            let mut headroom = limit - sign * position * quote.mid_price;
            if let Some(cap) = self.config.absolute_max_position_dollar {
                headroom = headroom.min(cap - sign * (position - baseline) * quote.mid_price);
            }
            for level in 0..self.config.num_levels.min(quote.num_levels) {
                let price = if side == Side::Buy { quote.bid_prices[level] } else { quote.ask_prices[level] };
                if !price.is_finite() || price <= 0.0 { continue; }
                // Reserve other pending/live/canceling slots. Mark conservatively
                // at max(limit price, mid) so appreciation cannot hide exposure.
                let orders = if side == Side::Buy { &self.bid_orders } else { &self.ask_orders };
                let reserved: f64 = orders.iter().enumerate()
                    .filter(|(index, _)| *index != level)
                    .filter_map(|(_, order)| order.as_ref())
                    .map(|order| order.quantity * order.price.max(quote.mid_price)).sum();
                let budget = (headroom - reserved).max(0.0);
                let mut qty = quote.quantity.min(budget / price.max(quote.mid_price));
                if let Some(cap) = self.config.max_order_qty_dollar { qty = qty.min(cap / price); }
                qty = (qty / self.config.lot_size).floor() * self.config.lot_size;
                if !qty.is_finite() || qty < self.config.min_order_qty || qty * price < self.config.min_order_qty_dollar {
                    qty = 0.0;
                }
                if let Some(order) = &orders[level] {
                    let exceeds = order.quantity * order.price.max(quote.mid_price) > budget + 1e-9
                        || self.config.max_order_qty_dollar.is_some_and(|cap| order.quantity * order.price > cap + 1e-9);
                    if (exceeds || qty == 0.0) && order.state != OrderState::Canceling {
                        let cl_ord_id = order.cl_ord_id.clone();
                        self.set_order_canceling(side, level, current_time_ns);
                        decisions.push(OrderDecision::Cancel { cl_ord_id });
                        continue;
                    }
                }
                if qty > 0.0 {
                    if let Some(decision) = self.process_side_level(side, level, price, qty, current_time_ns) {
                        decisions.push(decision);
                    }
                }
            }
        }
    }

    fn cancel_exposure(&mut self, decisions: &mut Vec<OrderDecision>, now: i64) {
        for side in [Side::Buy, Side::Sell] {
            for level in 0..MAX_ORDER_LEVELS {
                let orders = if side == Side::Buy { &self.bid_orders } else { &self.ask_orders };
                if let Some(order) = &orders[level] {
                    if order.state != OrderState::Canceling {
                        let cl_ord_id = order.cl_ord_id.clone();
                        self.set_order_canceling(side, level, now);
                        decisions.push(OrderDecision::Cancel { cl_ord_id });
                    }
                }
            }
        }
    }

    pub fn execution_state(&self) -> Arc<ExecutionState> { Arc::clone(&self.execution) }

    pub fn set_precision(&mut self, tick: f64, lot: f64, min_qty: f64) {
        self.config.tick_size = tick;
        self.config.lot_size = lot;
        self.config.min_order_qty = min_qty;
    }

    /// Process one side at a specific level and return decision.
    #[inline]
    fn process_side_level(
        &mut self,
        side: Side,
        level: usize,
        new_price: f64,
        qty: f64,
        current_time_ns: i64,
    ) -> Option<OrderDecision> {
        // Check order state without cloning - only clone cl_ord_id when needed
        let order = match side {
            Side::Buy => self.bid_orders[level].as_ref(),
            Side::Sell => self.ask_orders[level].as_ref(),
        };

        match order {
            None => {
                // No order at this level - place new one
                let cl_ord_id = self.generate_cl_ord_id(level);
                debug!(
                    "[{}] NEW {} L{} order: price={:.2}, qty={:.6}, id={}",
                    self.config.symbol, side, level, new_price, qty, cl_ord_id
                );
                self.set_order_pending(side, level, cl_ord_id.clone(), new_price, qty, current_time_ns);
                self.stats.orders_sent += 1;
                self.last_order_sent_at_ms = current_time_ns.max(0) as u64 / 1_000_000;
                Some(OrderDecision::Send {
                    side,
                    level,
                    price: new_price,
                    qty,
                    cl_ord_id,
                })
            }
            Some(o) if o.state == OrderState::Pending => {
                // Still pending - wait for confirmation
                let age_secs = (current_time_ns - o.sent_at_ns) / 1_000_000_000;
                debug!(
                    "[{}] {} L{} order blocked: pending confirmation for {} ({}s ago)",
                    self.config.symbol, side, level, o.cl_ord_id, age_secs
                );
                None
            }
            Some(o) if o.state == OrderState::Canceling => {
                // Cancel in progress - wait for confirmation
                let age_secs = (current_time_ns - o.sent_at_ns) / 1_000_000_000;
                debug!(
                    "[{}] {} L{} order blocked: cancel pending for {} ({}s ago, order_id={:?})",
                    self.config.symbol, side, level, o.cl_ord_id, age_secs, o.order_id
                );
                None
            }
            Some(o) => {
                // Live order - check if reprice needed
                let price_changed = self.should_reprice(o.price, new_price);
                let age_ns = current_time_ns - o.sent_at_ns;
                let order_too_old = self.config.max_live_age_ns > 0
                    && age_ns > self.config.max_live_age_ns as i64;

                let qty_changed = (o.quantity - qty).abs() >= self.config.lot_size * 0.5;
                if price_changed || order_too_old || qty_changed {
                    let change_bps = ((new_price - o.price) / o.price).abs() * 10_000.0;
                    let age_secs = age_ns / 1_000_000_000;

                    if order_too_old && !price_changed {
                        debug!(
                            "[{}] REFRESH {} L{} order (age={}s > {}s): {:.2} -> {:.2} ({:.1}bps), id={}",
                            self.config.symbol, side, level, age_secs,
                            self.config.max_live_age_ns / 1_000_000_000,
                            o.price, new_price, change_bps, o.cl_ord_id
                        );
                    } else {
                        debug!(
                            "[{}] REPRICE {} L{} order: {:.2} -> {:.2} ({:.1}bps), id={}",
                            self.config.symbol, side, level, o.price, new_price, change_bps, o.cl_ord_id
                        );
                    }
                    let cancel_id = o.cl_ord_id.clone(); // Only clone when actually repricing
                    self.set_order_canceling(side, level, current_time_ns);
                    self.stats.reprices += 1;


                    Some(OrderDecision::CancelAndReplace {
                        cancel_id,
                        level,
                        new_price,
                        qty,
                    })
                } else {
                    // Price within threshold and order not too old - no action
                    None
                }
            }
        }
    }

    /// Check if price change exceeds threshold.
    #[inline]
    fn should_reprice(&self, old_price: f64, new_price: f64) -> bool {
        if old_price <= 0.0 {
            return true;
        }
        let change_bps = ((new_price - old_price) / old_price).abs() * 10_000.0;
        change_bps >= self.config.reprice_threshold_bps
    }

    /// Check for and handle timed out orders.
    ///
    /// Simple logic: any order older than timeout is cleared immediately
    /// and a cancel request is sent. No complex state machine - just clear
    /// the slot so new orders can be placed.
    #[inline]
    fn check_timeouts(&mut self, current_time_ns: i64) -> Vec<OrderDecision> {
        let mut decisions = Vec::new();
        for orders in [&self.bid_orders, &self.ask_orders] {
            for order in orders.iter().flatten() {
                if order.state != OrderState::Live
                    && current_time_ns - order.sent_at_ns > self.config.pending_timeout_ns as i64 {
                    decisions.push(OrderDecision::Cancel { cl_ord_id: order.cl_ord_id.clone() });
                }
            }
        }
        if !decisions.is_empty() {
            self.stats.timeouts += decisions.len() as u64;
            self.begin_reconciliation();
        }
        decisions
    }

    /// Set order state to pending.
    fn set_order_pending(
        &mut self,
        side: Side,
        level: usize,
        cl_ord_id: String,
        price: f64,
        quantity: f64,
        sent_at_ns: i64,
    ) {
        let order = LiveOrder {
            cl_ord_id,
            order_id: None,
            side,
            level,
            price,
            quantity,
            state: OrderState::Pending,
            sent_at_ns,
            cancel_fail_count: 0,
        };

        match side {
            Side::Buy => self.bid_orders[level] = Some(order),
            Side::Sell => self.ask_orders[level] = Some(order),
        }
    }

    /// Set order state to canceling and reset the timeout clock.
    ///
    /// Updating `sent_at_ns` when entering Canceling state ensures that:
    /// 1. Timeout is measured from when cancel was initiated, not original order placement
    /// 2. If cancel fails and we revert to Live, the timeout won't fire prematurely
    fn set_order_canceling(&mut self, side: Side, level: usize, current_time_ns: i64) {
        let order = match side {
            Side::Buy => &mut self.bid_orders[level],
            Side::Sell => &mut self.ask_orders[level],
        };

        if let Some(o) = order {
            o.state = OrderState::Canceling;
            o.sent_at_ns = current_time_ns; // Reset timeout clock for cancel operation
        }
    }

    // ========== Event Handlers (called from async context) ==========

    /// Called when an order is accepted by the exchange.
    /// Returns false when the client ID is not tracked or conflicts with an
    /// exchange ID already bound to it. Callers must reconcile in that case.
    pub fn on_order_accepted(&mut self, cl_ord_id: &str, order_id: i64) -> bool {
        let existing_order_id = match self.find_order(cl_ord_id) {
            Some(order) => order.order_id,
            None => return false,
        };

        if order_id != 0
            && existing_order_id.is_some_and(|existing| existing != 0 && existing != order_id)
        {
            error!(
                "[{}] Order identity conflict for {}: local={:?}, exchange={}",
                self.config.symbol, cl_ord_id, existing_order_id, order_id
            );
            return false;
        }

        if order_id != 0
            && self.bid_orders.iter().chain(self.ask_orders.iter()).flatten()
                .any(|order| order.order_id == Some(order_id) && order.cl_ord_id != cl_ord_id)
        {
            error!(
                "[{}] Exchange order_id {} already bound to another slot; rejecting {}",
                self.config.symbol, order_id, cl_ord_id
            );
            return false;
        }

        // Reset circuit breaker only after correlating the acceptance.
        self.consecutive_rejections = 0;

        if order_id < 0 {
            warn!(
                "[{}] Order {} accepted with negative order_id={} - cancellation by order_id may fail",
                self.config.symbol, cl_ord_id, order_id
            );
        }

        if let Some(order) = self.find_order_mut(cl_ord_id) {
            let side = order.side;
            let prev_state = order.state;
            let sent_at_ns = order.sent_at_ns;

            // A correlated response-stream success has no numeric order ID and
            // uses zero as a sentinel. Keep None until REST supplies the ID.
            if order_id != 0 {
                order.order_id = Some(order_id);
            }

            // Only transition to Live if still Pending
            // If already Canceling, keep it Canceling (cancel is in flight)
            if prev_state == OrderState::Pending {
                order.state = OrderState::Live;
                self.stats.orders_accepted += 1;

                // Calculate acceptance latency
                use std::time::{SystemTime, UNIX_EPOCH};
                let now_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as i64;
                let latency_ms = (now_ns - sent_at_ns) / 1_000_000;

                debug!(
                    "[{}] {} order accepted: {} -> {} (latency={}ms)",
                    self.config.symbol, side, cl_ord_id, order_id, latency_ms
                );
            } else if prev_state == OrderState::Canceling && existing_order_id.is_none() {
                // A REST snapshot can supply the numeric ID after the
                // response-stream ACK was counted and a cancel was sent.
                // Preserve Canceling and do not count the same acceptance twice.
                debug!(
                    "[{}] {} order {} enriched while Canceling (order_id={})",
                    self.config.symbol, side, cl_ord_id, order_id
                );
            }
        }

        true
    }

    /// Called when an order is rejected.
    pub fn on_order_rejected(&mut self, cl_ord_id: &str, reason: &str) {
        // Get side and level before clearing
        let order_info = self.find_order(cl_ord_id).map(|o| (o.side, o.level));
        let side_str = order_info.map(|(s, l)| format!("{} L{} ", s, l)).unwrap_or_default();
        warn!(
            "[{}] {}order rejected: {} - {}",
            self.config.symbol, side_str, cl_ord_id, reason
        );
        self.clear_order_by_cl_ord_id(cl_ord_id);
        self.stats.rejections += 1;


        // Circuit breaker: pause trading after too many consecutive rejections
        self.consecutive_rejections += 1;
        if self.config.circuit_breaker_rejections > 0
            && self.consecutive_rejections >= self.config.circuit_breaker_rejections
        {
            error!(
                "[{}] CIRCUIT BREAKER: {} consecutive rejections - PAUSING trading",
                self.config.symbol, self.consecutive_rejections
            );
            self.set_pause_reason(PauseReason::CircuitBreaker);
            // Record when circuit breaker was triggered
            self.circuit_breaker_triggered_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
        }
    }

    /// Reset circuit breaker. Trading resumes only if no other pause reasons are active.
    pub fn reset_circuit_breaker(&mut self) {
        self.consecutive_rejections = 0;
        self.circuit_breaker_triggered_at_ms = 0;
        self.clear_pause_reason(PauseReason::CircuitBreaker);
        let remaining = self.execution.pause_reasons.load(Ordering::Acquire);
        if remaining == 0 {
            info!("[{}] Circuit breaker reset, trading resumed", self.config.symbol);
        } else {
            info!("[{}] Circuit breaker reset, but still paused (reasons: {:#04b})", self.config.symbol, remaining);
        }
    }

    /// True once per trip when the cooldown has elapsed. The caller then
    /// requests account reconciliation; trading resumes only if it verifies.
    pub fn check_circuit_breaker_recovery(&mut self, recovery_secs: u64) -> bool {
        self.has_pause_reason(PauseReason::CircuitBreaker)
            && Self::cooldown_elapsed(&mut self.circuit_breaker_triggered_at_ms, recovery_secs)
    }

    fn cooldown_elapsed(triggered_at_ms: &mut u64, recovery_secs: u64) -> bool {
        if recovery_secs == 0 || *triggered_at_ms == 0 {
            return false;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if now_ms.saturating_sub(*triggered_at_ms) < recovery_secs.saturating_mul(1000) {
            return false;
        }
        *triggered_at_ms = 0;
        true
    }

    /// Trigger a safety pause from external signal (e.g., >2 orders detected).
    ///
    /// This is called when the OpenOrdersChecker detects more than 2 orders
    /// on the exchange, indicating a duplicate/stuck state that requires
    /// immediate intervention.
    pub fn trigger_safety_pause(&mut self, reason: &str) {
        error!(
            "[{}] SAFETY PAUSE: {} - pausing trading",
            self.config.symbol, reason
        );
        self.set_pause_reason(PauseReason::Safety);
        self.safety_pause_triggered_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
    }

    /// Safety pauses (duplicate orders, transport faults) use the same
    /// cooldown-then-reconcile recovery with their own timer.
    pub fn check_safety_pause_recovery(&mut self, recovery_secs: u64) -> bool {
        self.has_pause_reason(PauseReason::Safety)
            && Self::cooldown_elapsed(&mut self.safety_pause_triggered_at_ms, recovery_secs)
    }

    /// Get consecutive rejection count.
    pub fn consecutive_rejections(&self) -> u32 {
        self.consecutive_rejections
    }

    /// Get seconds since last order was sent.
    /// Returns None if no order has ever been sent.
    pub fn seconds_since_last_order(&self) -> Option<u64> {
        if self.last_order_sent_at_ms == 0 {
            return None;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Some((now_ms - self.last_order_sent_at_ms) / 1000)
    }

    /// Called when an order is canceled.
    pub fn on_order_canceled(&mut self, order_id: i64) -> bool {
        // Get info for logging before clearing - search all levels
        let mut order_info: Option<(String, Side, usize, OrderState)> = None;

        for level in 0..MAX_ORDER_LEVELS {
            if let Some(o) = &self.bid_orders[level] {
                if o.order_id == Some(order_id) {
                    order_info = Some((o.cl_ord_id.clone(), o.side, level, o.state));
                    break;
                }
            }
            if let Some(o) = &self.ask_orders[level] {
                if o.order_id == Some(order_id) {
                    order_info = Some((o.cl_ord_id.clone(), o.side, level, o.state));
                    break;
                }
            }
        }

        if let Some((cl_ord_id, side, level, state)) = order_info {
            info!(
                "[{}] {} L{} order canceled: {} (order_id={}, was {:?}) - slot freed",
                self.config.symbol, side, level, cl_ord_id, order_id, state
            );
            self.clear_order_by_exchange_id(order_id);
            self.stats.orders_canceled += 1;
            true
        } else {
            // Order not found - may have been force-cleared or already canceled
            debug!(
                "[{}] Cancel confirmation for unknown order_id={} - already cleared?",
                self.config.symbol, order_id
            );
            false
        }
    }

    /// Called when an order cancel is confirmed by client order ID.
    pub fn on_order_canceled_by_cl_ord_id(&mut self, cl_ord_id: &str) -> bool {
        // Get info for logging before clearing
        let order_info = self.find_order(cl_ord_id).map(|o| (o.side, o.level, o.state));

        if let Some((side, level, state)) = order_info {
            info!(
                "[{}] {} L{} order canceled: {} (was {:?}) - slot freed",
                self.config.symbol, side, level, cl_ord_id, state
            );
            self.clear_order_by_cl_ord_id(cl_ord_id);
            self.stats.orders_canceled += 1;
            true
        } else {
            debug!(
                "[{}] Cancel confirmation for unknown cl_ord_id={} - already cleared?",
                self.config.symbol, cl_ord_id
            );
            false
        }
    }

    /// Apply a correlated fill. Full fills free the slot. Partial fills keep the
    /// slot with its full quantity reserved until reconciliation verifies the remainder.
    pub fn on_order_fill(
        &mut self,
        order_id: i64,
        cl_ord_id: Option<&str>,
        _fill_quantity: f64,
        fully_filled: bool,
    ) -> bool {
        let mut matched_slot = None;
        for level in 0..MAX_ORDER_LEVELS {
            for (side, slot) in [
                (Side::Buy, &self.bid_orders[level]),
                (Side::Sell, &self.ask_orders[level]),
            ] {
                if slot.as_ref().is_some_and(|order| {
                    (order_id != 0 && order.order_id == Some(order_id))
                        || cl_ord_id.is_some_and(|id| order.cl_ord_id == id)
                }) {
                    matched_slot = Some((side, level));
                    break;
                }
            }
            if matched_slot.is_some() {
                break;
            }
        }

        if let Some((side, level)) = matched_slot {
            self.require_position_refresh_after_fill();
            if fully_filled {
                match side {
                    Side::Buy => self.bid_orders[level] = None,
                    Side::Sell => self.ask_orders[level] = None,
                }
            } else {
                // The exchange fill quantity may be cumulative. Keep the slot
                // intact until reconciliation cancels and verifies the remainder.
            }
            true
        } else {
            false
        }
    }

    /// Called when a cancel request fails.
    ///
    /// The order is still live on the exchange, so we revert from Canceling
    /// back to Live state to allow repricing on the next quote.
    /// Maximum consecutive cancel failures before forcefully clearing the slot.
    /// This prevents infinite cancel loops when order_id is invalid (e.g., 0).
    const MAX_CANCEL_FAILURES: u32 = 3;

    pub fn on_cancel_failed(&mut self, order_id: i64, reason: &str) {
        warn!("[{}] Cancel failed for order {}: {}", self.config.symbol, order_id, reason);
        let symbol = &self.config.symbol;
        let mut fail_closed = false;
        for order in self.bid_orders.iter_mut().chain(self.ask_orders.iter_mut()).flatten() {
            if order.order_id != Some(order_id) || order.state != OrderState::Canceling {
                continue;
            }
            order.cancel_fail_count += 1;
            if order.cancel_fail_count >= Self::MAX_CANCEL_FAILURES {
                // Repeated failures are ambiguous. Keep the slot and fail closed;
                // reconciliation will clear it only after REST verification.
                error!(
                    "[{}] {} L{} order {} exceeded {} cancel failures - pausing for reconciliation",
                    symbol, order.side, order.level, order.cl_ord_id, Self::MAX_CANCEL_FAILURES
                );
                fail_closed = true;
            } else {
                order.state = OrderState::Live;
                debug!(
                    "[{}] Reverted {} L{} order {} to Live state (fail count: {})",
                    symbol, order.side, order.level, order_id, order.cancel_fail_count
                );
            }
            break;
        }
        if fail_closed {
            self.set_pause_reason(PauseReason::Safety);
            self.set_pause_reason(PauseReason::Reconciliation);
        }
    }

    // ========== Pending Order Handling ==========

    fn find_order(&self, cl_ord_id: &str) -> Option<&LiveOrder> {
        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.bid_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    return Some(order);
                }
            }
            if let Some(order) = &self.ask_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    return Some(order);
                }
            }
        }
        None
    }

    /// Whether a client order ID currently occupies a local slot.
    /// Used only by REST reconciliation on the cold path.
    pub fn has_tracked_order(&self, cl_ord_id: &str) -> bool {
        self.find_order(cl_ord_id).is_some()
    }

    /// Find a REST-identified local order that is absent from a later REST
    /// open-orders snapshot. Absence can mean a fill, so callers must refresh
    /// position state before quoting again.
    pub fn first_missing_rest_order(&self, observed_order_ids: &[i64]) -> Option<(String, i64)> {
        for level in 0..MAX_ORDER_LEVELS {
            for order in [&self.bid_orders[level], &self.ask_orders[level]]
                .into_iter()
                .flatten()
            {
                if let Some(order_id) = order.order_id {
                    if !observed_order_ids.contains(&order_id) {
                        return Some((order.cl_ord_id.clone(), order_id));
                    }
                }
            }
        }
        None
    }

    /// Find an accepted order that REST has not identified within the allowed
    /// propagation window. Such an order may have filled or been rejected
    /// before the first open-orders poll observed it.
    pub fn first_stale_unconfirmed_rest_order(
        &self,
        observed_cl_ord_ids: &[&str],
        observed_at_ns: i64,
        grace_ns: i64,
    ) -> Option<String> {
        for level in 0..MAX_ORDER_LEVELS {
            for order in [&self.bid_orders[level], &self.ask_orders[level]]
                .into_iter()
                .flatten()
            {
                if order.order_id.is_none()
                    && !observed_cl_ord_ids.contains(&order.cl_ord_id.as_str())
                    && observed_at_ns.saturating_sub(order.sent_at_ns) > grace_ns
                {
                    return Some(order.cl_ord_id.clone());
                }
            }
        }
        None
    }

    #[inline]
    fn find_order_mut(&mut self, cl_ord_id: &str) -> Option<&mut LiveOrder> {
        // First find which array and level contains the order
        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.bid_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    return self.bid_orders[level].as_mut();
                }
            }
            if let Some(order) = &self.ask_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    return self.ask_orders[level].as_mut();
                }
            }
        }
        None
    }

    #[inline]
    fn clear_order_by_cl_ord_id(&mut self, cl_ord_id: &str) {
        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.bid_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    self.bid_orders[level] = None;
                    return;
                }
            }
            if let Some(order) = &self.ask_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    self.ask_orders[level] = None;
                    return;
                }
            }
        }
    }

    #[inline]
    fn clear_order_by_exchange_id(&mut self, order_id: i64) {
        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.bid_orders[level] {
                if order.order_id == Some(order_id) {
                    self.bid_orders[level] = None;
                    return;
                }
            }
            if let Some(order) = &self.ask_orders[level] {
                if order.order_id == Some(order_id) {
                    self.ask_orders[level] = None;
                    return;
                }
            }
        }
    }

    /// Roll back state when a decision definitely never reached the executor.
    pub fn on_dispatch_failed(&mut self, decision: &OrderDecision) {
        match decision {
            OrderDecision::Send { cl_ord_id, .. } => {
                self.clear_order_by_cl_ord_id(cl_ord_id);
            }
            OrderDecision::Cancel { cl_ord_id }
            | OrderDecision::CancelAndReplace {
                cancel_id: cl_ord_id,
                ..
            } => {
                for order in self
                    .bid_orders
                    .iter_mut()
                    .chain(self.ask_orders.iter_mut())
                    .flatten()
                {
                    if order.cl_ord_id == *cl_ord_id {
                        order.state = OrderState::Live;
                    }
                }
            }
            OrderDecision::NoAction => {}
        }
    }

    // ========== Pause/Resume (for WebSocket disconnection) ==========

    /// Pause trading due to WebSocket disconnection.
    pub fn pause(&self) {
        self.set_pause_reason(PauseReason::OrderWebSocket);
        info!("[{}] Order manager paused (WebSocket disconnected)", self.config.symbol);
    }

    /// Resume trading after WebSocket reconnection.
    /// Only clears the WS disconnect pause reason; trading stays paused if
    /// circuit breaker or safety pause is also active.
    pub fn resume(&self) {
        self.clear_pause_reason(PauseReason::OrderWebSocket);
        let remaining = self.execution.pause_reasons.load(Ordering::Acquire);
        if remaining == 0 {
            info!("[{}] Order manager resumed (WebSocket reconnected)", self.config.symbol);
        } else {
            info!("[{}] WS reconnected but still paused (reasons: {:#04b})", self.config.symbol, remaining);
        }
    }

    /// Check if trading is paused (any reason).
    pub fn is_paused(&self) -> bool {
        self.execution.pause_reasons.load(Ordering::Acquire) != 0
    }

    /// Set one independent pause reason.
    #[inline]
    pub fn set_pause_reason(&self, reason: PauseReason) {
        let old = self.execution.pause_reasons.fetch_or(reason as u8, Ordering::AcqRel);
        if old & reason as u8 == 0 {
            self.execution.generation.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Clear one independent pause reason.
    #[inline]
    pub fn clear_pause_reason(&self, reason: PauseReason) {
        self.execution.pause_reasons.fetch_and(!(reason as u8), Ordering::Release);
    }

    /// Test one pause reason.
    #[inline]
    pub fn has_pause_reason(&self, reason: PauseReason) -> bool {
        self.execution.pause_reasons.load(Ordering::Acquire) & reason as u8 != 0
    }

    /// Return the active pause bitfield for diagnostics and tests.
    #[inline]
    pub fn pause_reason_bits(&self) -> u8 {
        self.execution.pause_reasons.load(Ordering::Acquire)
    }

    /// Capture the first fresh position as the incremental exposure baseline.
    pub fn initialize_position_baseline(&mut self) {
        if self.position_baseline.is_none() {
            self.position_baseline = Some(self.position.get());
        }
    }

    /// Block new decisions until two successful position publications occur
    /// after a fill. This is conservative across REST eventual consistency.
    pub fn require_position_refresh_after_fill(&mut self) {
        let required = self.position.update_version().saturating_add(2);
        self.resume_after_position_version = Some(
            self.resume_after_position_version
                .map(|current| current.max(required))
                .unwrap_or(required),
        );
        self.set_pause_reason(PauseReason::RiskData);
    }

    /// Called from the one-second cold risk loop, never from quote processing.
    pub fn position_refresh_after_fill_complete(&mut self) -> bool {
        let Some(required) = self.resume_after_position_version else {
            return true;
        };
        if self.position.update_version() < required {
            return false;
        }
        self.resume_after_position_version = None;
        true
    }

    /// Enter reconciliation before any exchange cleanup begins.
    pub fn begin_reconciliation(&self) {
        self.set_pause_reason(PauseReason::Reconciliation);
    }

    /// Apply a verified account-wide reconciliation result.
    pub fn finish_reconciliation(&mut self) {
        self.retired_generation = self.execution.generation();
        self.clear_all_orders();
        self.safety_pause_triggered_at_ms = 0;
        self.circuit_breaker_triggered_at_ms = 0;
        self.consecutive_rejections = 0;
        self.clear_pause_reason(PauseReason::Safety);
        self.clear_pause_reason(PauseReason::CircuitBreaker);
        self.clear_pause_reason(PauseReason::Reconciliation);
    }

    // ========== Shutdown ==========

    /// Initiate graceful shutdown - stop placing new orders.
    pub fn shutdown(&self) {
        self.execution.shutdown.store(true, Ordering::Release);
        self.execution.generation.fetch_add(1, Ordering::AcqRel);
        info!("[{}] Order manager shutdown initiated", self.config.symbol);
    }

    /// Check if shutdown is in progress.
    pub fn is_shutting_down(&self) -> bool {
        self.execution.shutdown.load(Ordering::Acquire)
    }

    /// Get all tracked order client IDs for batch cancel on shutdown.
    ///
    /// Canceling orders remain potentially live until the exchange confirms
    /// their absence, so they must be included here too.
    pub fn get_all_live_order_ids(&self) -> Vec<String> {
        let mut ids = Vec::with_capacity(MAX_ORDER_LEVELS * 2);

        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.bid_orders[level] {
                ids.push(order.cl_ord_id.clone());
            }
            if let Some(order) = &self.ask_orders[level] {
                ids.push(order.cl_ord_id.clone());
            }
        }

        ids
    }

    /// Clear all tracked orders from internal state.
    ///
    /// Use this after canceling all orders via HTTP to ensure the order manager's
    /// internal state is synchronized. This prevents stale orders from blocking
    /// new order placement after reconnection.
    pub fn clear_all_orders(&mut self) {
        for level in 0..MAX_ORDER_LEVELS {
            if self.bid_orders[level].is_some() {
                debug!("[{}] Clearing bid L{} order from state", self.config.symbol, level);
                self.bid_orders[level] = None;
            }
            if self.ask_orders[level].is_some() {
                debug!("[{}] Clearing ask L{} order from state", self.config.symbol, level);
                self.ask_orders[level] = None;
            }
        }
    }

    pub fn bid_order_at(&self, level: usize) -> Option<&LiveOrder> {
        self.bid_orders.get(level).and_then(Option::as_ref)
    }

    /// Get ask order at a specific level.
    pub fn ask_order_at(&self, level: usize) -> Option<&LiveOrder> {
        if level < MAX_ORDER_LEVELS {
            self.ask_orders[level].as_ref()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_position() -> Arc<SharedPosition> {
        Arc::new(SharedPosition::new("TEST-USD".to_string()))
    }

    /// Create SharedEquity with a given max_position_dollar.
    /// Uses reverse formula: equity = max_position / (0.82 * 0.9), above the minimum-order floor
    fn create_test_equity(max_position_dollar: f64) -> Arc<SharedEquity> {
        let equity = Arc::new(SharedEquity::new(1, 10.0, 1.0));
        // Reserve 18% of equity for one side of orders, then apply the 10% buffer.
        let raw_equity = max_position_dollar / (0.82 * 0.9);
        equity.set_equity(raw_equity);
        equity
    }

    /// Create SharedEquity with default high limit (won't trigger position limits in tests).
    fn create_test_equity_high_limit() -> Arc<SharedEquity> {
        create_test_equity(100_000_000.0) // $100M max position
    }

    fn create_test_quote(bid: f64, ask: f64, qty: f64) -> Quote {
        Quote {
            symbol: "TEST-USD".into(),
            bid_prices: [bid, bid - 1.0], // Level 1 is 1.0 lower
            ask_prices: [ask, ask + 1.0], // Level 1 is 1.0 higher
            num_levels: 1, // Default to 1 level for backward compatibility
            quantity: qty,
            mid_price: (bid + ask) / 2.0,
            spread: ask - bid,
            volatility: 0.001,
            alpha: 0.0,
            position: 0.0,
            half_spread_tick: 1.0,
            valid_for_trading: true,
            history_secs: 600.0,
            bid_floored: [false, false],
            ask_floored: [false, false],
        }
    }

    fn create_test_quote_2_levels(bid: f64, ask: f64, qty: f64) -> Quote {
        Quote {
            symbol: "TEST-USD".into(),
            bid_prices: [bid, bid - 10.0], // Level 1 is wider
            ask_prices: [ask, ask + 10.0], // Level 1 is wider
            num_levels: 2,
            quantity: qty,
            mid_price: (bid + ask) / 2.0,
            spread: ask - bid,
            volatility: 0.001,
            alpha: 0.0,
            position: 0.0,
            half_spread_tick: 1.0,
            valid_for_trading: true,
            history_secs: 600.0,
            bid_floored: [false, false],
            ask_floored: [false, false],
        }
    }

    #[test]
    fn test_initial_order_placement() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let config = OrderManagerConfig::default();
        let mut manager = OrderManager::new(config, position, equity);

        let quote = create_test_quote(99000.0, 101000.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);

        // Should place both bid and ask
        assert_eq!(decisions.len(), 2);

        let has_buy = decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Buy, .. }));
        let has_sell = decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Sell, .. }));

        assert!(has_buy, "Should have buy order");
        assert!(has_sell, "Should have sell order");
    }

    #[test]
    fn request_ack_can_be_enriched_by_rest_without_double_counting() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let mut manager = OrderManager::new(OrderManagerConfig::default(), position, equity);
        let decisions = manager.on_quote(
            &create_test_quote(99_000.0, 101_000.0, 0.001),
            1_000_000_000,
        );
        let cl_ord_id = decisions
            .iter()
            .find_map(|decision| match decision {
                OrderDecision::Send {
                    side: Side::Buy,
                    cl_ord_id,
                    ..
                } => Some(cl_ord_id.clone()),
                _ => None,
            })
            .unwrap();

        assert!(manager.on_order_accepted(&cl_ord_id, 0));
        assert_eq!(manager.find_order(&cl_ord_id).unwrap().state, OrderState::Live);
        assert_eq!(manager.find_order(&cl_ord_id).unwrap().order_id, None);
        assert_eq!(manager.stats().orders_accepted, 1);
        let all_client_ids: Vec<&str> = decisions
            .iter()
            .filter_map(|decision| match decision {
                OrderDecision::Send { cl_ord_id, .. } => Some(cl_ord_id.as_str()),
                _ => None,
            })
            .collect();
        assert!(manager
            .first_stale_unconfirmed_rest_order(
                &all_client_ids,
                11_000_000_000,
                9_000_000_000,
            )
            .is_none());
        assert!(manager
            .first_stale_unconfirmed_rest_order(
                &[],
                11_000_000_000,
                9_000_000_000,
            )
            .is_some());

        assert!(manager.on_order_accepted(&cl_ord_id, 42));
        assert_eq!(manager.find_order(&cl_ord_id).unwrap().order_id, Some(42));
        assert_eq!(manager.stats().orders_accepted, 1);
        assert!(manager.first_missing_rest_order(&[42]).is_none());
        assert_eq!(
            manager.first_missing_rest_order(&[]),
            Some((cl_ord_id.clone(), 42))
        );
        assert!(!manager.on_order_accepted(&cl_ord_id, 43));
        assert!(!manager.on_order_accepted("mm_TEST-USD_unknown", 44));
    }

    #[test]
    fn rest_enrichment_while_canceling_does_not_double_count() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let mut manager = OrderManager::new(OrderManagerConfig::default(), position, equity);
        let decisions = manager.on_quote(
            &create_test_quote(99_000.0, 101_000.0, 0.001),
            1_000_000_000,
        );
        let cl_ord_id = decisions
            .iter()
            .find_map(|decision| match decision {
                OrderDecision::Send {
                    side: Side::Buy,
                    cl_ord_id,
                    ..
                } => Some(cl_ord_id.clone()),
                _ => None,
            })
            .unwrap();

        assert!(manager.on_order_accepted(&cl_ord_id, 0));
        assert_eq!(manager.stats().orders_accepted, 1);
        manager.set_order_canceling(Side::Buy, 0, 2_000_000_000);

        assert!(manager.on_order_accepted(&cl_ord_id, 42));
        let order = manager.find_order(&cl_ord_id).unwrap();
        assert_eq!(order.state, OrderState::Canceling);
        assert_eq!(order.order_id, Some(42));
        assert_eq!(manager.stats().orders_accepted, 1);
    }

    #[test]
    fn test_no_reprice_within_threshold() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let config = OrderManagerConfig {
            reprice_threshold_bps: 10.0,
            ..OrderManagerConfig::default()
        };
        let mut manager = OrderManager::new(config, position, equity);

        // Place initial orders
        let quote = create_test_quote(100000.0, 100010.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);
        assert_eq!(decisions.len(), 2);

        // Simulate acceptance
        if let Some(OrderDecision::Send { cl_ord_id, .. }) = decisions.first() {
            manager.on_order_accepted(cl_ord_id, 1001);
        }
        if let Some(OrderDecision::Send { cl_ord_id, .. }) = decisions.get(1) {
            manager.on_order_accepted(cl_ord_id, 1002);
        }

        // Small price change within threshold (5 bps)
        let quote2 = create_test_quote(100005.0, 100015.0, 0.001);
        let decisions2 = manager.on_quote(&quote2, 2_000_000_000);

        // Should not reprice
        assert_eq!(decisions2.len(), 0, "Should not reprice within threshold");
    }

    #[test]
    fn test_reprice_beyond_threshold() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let config = OrderManagerConfig {
            reprice_threshold_bps: 1.0,
            ..OrderManagerConfig::default()
        };
        let mut manager = OrderManager::new(config, position, equity);

        // Place initial orders
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);

        // Simulate acceptance
        for (index, decision) in decisions.iter().enumerate() {
            if let OrderDecision::Send { cl_ord_id, .. } = decision {
                assert!(manager.on_order_accepted(cl_ord_id, 1001 + index as i64));
            }
        }

        // Large price change (100 bps): both live orders are replaced at the new
        // prices and wait for cancel confirmation.
        let quote2 = create_test_quote(101000.0, 101100.0, 0.001);
        let mut replaced: Vec<f64> = manager.on_quote(&quote2, 2_000_000_000).iter().map(|d| match d {
            OrderDecision::CancelAndReplace { new_price, .. } => *new_price,
            other => panic!("unexpected {other:?}"),
        }).collect();
        replaced.sort_by(f64::total_cmp);
        assert_eq!(replaced, vec![101000.0, 101100.0]);
        assert_eq!(manager.bid_order_at(0).unwrap().state, OrderState::Canceling);
        assert_eq!(manager.ask_order_at(0).unwrap().state, OrderState::Canceling);
        assert_eq!(manager.stats().reprices, 2);
    }

    #[test]
    fn test_position_limit_long() {
        let position = create_test_position();
        position.set(0.005); // 0.005 BTC position

        // Set max_position to $500 via SharedEquity
        let equity = create_test_equity(500.0);
        let config = OrderManagerConfig::default();
        let mut manager = OrderManager::new(config, position, equity);

        // Quote at $100,000 means position = $500 (at max)
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);

        // Should only place ask (sell), not bid (buy) since at max long
        assert!(decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Sell, .. })));
        assert!(!decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Buy, .. })));
    }

    #[test]
    fn test_position_limit_cancel_is_not_repeated() {
        let position = create_test_position();
        let equity = create_test_equity(500.0);
        let config = OrderManagerConfig::default();
        let mut manager = OrderManager::new(config, Arc::clone(&position), equity);

        // Place both sides while below the position limit.
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);
        for (idx, decision) in decisions.iter().enumerate() {
            if let OrderDecision::Send { cl_ord_id, .. } = decision {
                manager.on_order_accepted(cl_ord_id, 2000 + idx as i64);
            }
        }

        // Move over max long; the bid should be canceled exactly once and
        // marked Canceling so the next quote does not emit a duplicate cancel.
        position.set(0.006);
        let first = manager.on_quote(&quote, 2_000_000_000);
        let second = manager.on_quote(&quote, 3_000_000_000);

        assert_eq!(
            first.iter()
                .filter(|d| matches!(d, OrderDecision::Cancel { .. }))
                .count(),
            1,
            "first over-limit quote should cancel the bid"
        );
        assert!(
            second.iter()
                .all(|d| !matches!(d, OrderDecision::Cancel { .. })),
            "cancel should not repeat while the order is Canceling"
        );
    }

    #[test]
    fn test_cancel_handlers_report_real_matches_only() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let config = OrderManagerConfig::default();
        let mut manager = OrderManager::new(config, position, equity);

        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);
        let first_cl_ord_id = match &decisions[0] {
            OrderDecision::Send { cl_ord_id, .. } => cl_ord_id.clone(),
            _ => panic!("expected send decision"),
        };
        manager.on_order_accepted(&first_cl_ord_id, 3001);

        assert!(!manager.on_order_canceled(9999));
        assert_eq!(manager.stats().orders_canceled, 0);

        assert!(manager.on_order_canceled(3001));
        assert_eq!(manager.stats().orders_canceled, 1);

        assert!(!manager.on_order_canceled_by_cl_ord_id(&first_cl_ord_id));
        assert_eq!(manager.stats().orders_canceled, 1);
    }

    #[test]
    fn test_shutdown() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let config = OrderManagerConfig::default();
        let mut manager = OrderManager::new(config, position, equity);

        // Place initial orders
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let _decisions = manager.on_quote(&quote, 1_000_000_000);

        // Shutdown
        manager.shutdown();
        assert!(manager.is_shutting_down());

        // Should not place any orders after shutdown
        let decisions2 = manager.on_quote(&quote, 2_000_000_000);
        assert!(decisions2.is_empty(), "Should not place orders after shutdown");
    }

    #[test]
    fn pause_reasons_clear_independently() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let manager = OrderManager::new(OrderManagerConfig::default(), position, equity);

        manager.set_pause_reason(PauseReason::OrderWebSocket);
        manager.set_pause_reason(PauseReason::MarketData);
        manager.clear_pause_reason(PauseReason::OrderWebSocket);

        assert!(manager.is_paused());
        assert!(manager.has_pause_reason(PauseReason::MarketData));
        assert!(!manager.has_pause_reason(PauseReason::OrderWebSocket));
    }

    #[test]
    fn incremental_cap_accounts_for_position_and_live_same_side_orders() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let config = OrderManagerConfig {
            num_levels: 2,
            lot_size: 0.00001,
            min_order_qty: 0.00001,
            absolute_max_position_dollar: Some(50.0),
            position_limit_from_start: true,
            max_order_qty_dollar: Some(25.0),
            ..OrderManagerConfig::default()
        };
        let mut manager = OrderManager::new(config, Arc::clone(&position), equity);
        manager.initialize_position_baseline();
        let quote = create_test_quote_2_levels(99_999.0, 100_001.0, 0.00025);

        let decisions = manager.on_quote(&quote, 1_000_000_000);
        assert_eq!(decisions.len(), 4);
        let mut filled_client_id = None;
        for (index, decision) in decisions.into_iter().enumerate() {
            if let OrderDecision::Send {
                side,
                cl_ord_id,
                ..
            } = decision
            {
                manager.on_order_accepted(&cl_ord_id, 100 + index as i64);
                if side == Side::Buy && filled_client_id.is_none() {
                    filled_client_id = Some((100 + index as i64, cl_ord_id));
                }
            }
        }

        let (order_id, client_id) = filled_client_id.unwrap();
        assert!(manager.on_order_fill(order_id, Some(&client_id), 0.00025, true));
        assert!(manager.has_pause_reason(PauseReason::RiskData));
        position.set(0.00025);
        assert!(!manager.position_refresh_after_fill_complete());
        position.set(0.00025);
        assert!(manager.position_refresh_after_fill_complete());
        manager.clear_pause_reason(PauseReason::RiskData);
        let refill = manager.on_quote(&quote, 2_000_000_000);
        assert!(!refill.iter().any(|decision| matches!(
            decision,
            OrderDecision::Send {
                side: Side::Buy,
                ..
            }
        )));
    }

    #[test]
    fn partial_fill_keeps_slot_and_requires_position_refresh() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let mut manager = OrderManager::new(
            OrderManagerConfig::default(),
            Arc::clone(&position),
            equity,
        );
        let quote = create_test_quote(99_999.0, 100_001.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);
        let (client_id, order_id) = decisions
            .into_iter()
            .find_map(|decision| match decision {
                OrderDecision::Send {
                    side: Side::Buy,
                    cl_ord_id,
                    ..
                } => Some((cl_ord_id, 42)),
                _ => None,
            })
            .unwrap();
        manager.on_order_accepted(&client_id, order_id);

        assert!(manager.on_order_fill(order_id, Some(&client_id), 0.0004, false));
        assert!(manager.find_order(&client_id).is_some());
        assert!(manager.has_pause_reason(PauseReason::RiskData));
        position.set(0.0004);
        assert!(!manager.position_refresh_after_fill_complete());
        position.set(0.0004);
        assert!(manager.position_refresh_after_fill_complete());
    }

    #[test]
    fn test_two_level_order_placement() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let config = OrderManagerConfig {
            num_levels: 2,
            ..OrderManagerConfig::default()
        };
        let mut manager = OrderManager::new(config, position, equity);

        // Create a 2-level quote
        let quote = create_test_quote_2_levels(99000.0, 101000.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);

        // Four orders at the quoted level prices and quantity.
        let mut sent: Vec<(Side, usize, f64, f64)> = decisions.iter().map(|d| match d {
            OrderDecision::Send { side, level, price, qty, .. } => (*side, *level, *price, *qty),
            other => panic!("unexpected {other:?}"),
        }).collect();
        sent.sort_by_key(|(side, level, ..)| (*side == Side::Sell, *level));
        assert_eq!(sent, vec![
            (Side::Buy, 0, 99000.0, 0.001), (Side::Buy, 1, 98990.0, 0.001),
            (Side::Sell, 0, 101000.0, 0.001), (Side::Sell, 1, 101010.0, 0.001),
        ]);
    }

    #[test]
    fn test_two_level_position_limit_cancels_both() {
        let position = create_test_position();
        let equity = create_test_equity(500.0);
        let config = OrderManagerConfig { num_levels: 2, ..OrderManagerConfig::default() };
        let mut manager = OrderManager::new(config, Arc::clone(&position), equity);

        // Flat: all four orders go live.
        let quote = create_test_quote_2_levels(100000.0, 100100.0, 0.001);
        for (index, decision) in manager.on_quote(&quote, 1_000_000_000).iter().enumerate() {
            let OrderDecision::Send { cl_ord_id, .. } = decision else { panic!("unexpected {decision:?}") };
            assert!(manager.on_order_accepted(cl_ord_id, 1001 + index as i64));
        }

        // $600 long against a $500 limit: both bids are canceled, asks stay untouched.
        position.set(0.006);
        let decisions = manager.on_quote(&quote, 2_000_000_000);
        assert_eq!(decisions.len(), 2, "{decisions:?}");
        assert!(decisions.iter().all(|d| matches!(d, OrderDecision::Cancel { .. })));
        for level in 0..2 {
            assert_eq!(manager.bid_order_at(level).unwrap().state, OrderState::Canceling);
            assert_eq!(manager.ask_order_at(level).unwrap().state, OrderState::Live);
        }
        assert!(manager.on_quote(&quote, 3_000_000_000).is_empty(), "cancel is not repeated");
    }

    #[test]
    fn duplicate_exchange_id_is_rejected_and_cancel_failures_fail_closed() {
        let mut manager = OrderManager::new(OrderManagerConfig::default(), create_test_position(), create_test_equity_high_limit());
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let ids: Vec<String> = manager.on_quote(&quote, 1_000_000_000).into_iter().map(|d| match d {
            OrderDecision::Send { cl_ord_id, .. } => cl_ord_id,
            other => panic!("unexpected {other:?}"),
        }).collect();
        assert!(manager.on_order_accepted(&ids[0], 7));
        assert!(!manager.on_order_accepted(&ids[1], 7), "one exchange id cannot own two slots");
        assert!(manager.on_order_accepted(&ids[1], 8));

        // Reprice puts both orders into Canceling; two cancel failures revert the
        // bid for retry, the third fails closed and pauses for reconciliation.
        let quote2 = create_test_quote(101000.0, 101100.0, 0.001);
        assert_eq!(manager.on_quote(&quote2, 2_000_000_000).len(), 2);
        for expected_count in 1..3 {
            manager.on_cancel_failed(7, "busy");
            let bid = manager.bid_order_at(0).unwrap();
            assert_eq!((bid.state, bid.cancel_fail_count), (OrderState::Live, expected_count));
            assert_eq!(manager.on_quote(&quote2, 3_000_000_000).len(), 1, "cancel is re-issued");
        }
        manager.on_cancel_failed(7, "busy");
        assert_eq!(manager.bid_order_at(0).unwrap().state, OrderState::Canceling);
        assert!(manager.has_pause_reason(PauseReason::Safety));
        assert!(manager.has_pause_reason(PauseReason::Reconciliation));
        assert!(manager.on_quote(&quote2, 4_000_000_000).is_empty(), "paused");
    }

    #[test]
    fn circuit_breaker_recovers_only_after_cooldown_and_verified_reconciliation() {
        let mut manager = OrderManager::new(OrderManagerConfig::default(), create_test_position(), create_test_equity_high_limit());
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        for attempt in 0..3_i64 {
            for decision in manager.on_quote(&quote, 1_000_000_000 + attempt) {
                let OrderDecision::Send { cl_ord_id, .. } = decision else { panic!("unexpected {decision:?}") };
                manager.on_order_rejected(&cl_ord_id, "post-only would cross");
            }
        }
        assert!(manager.has_pause_reason(PauseReason::CircuitBreaker));
        assert!(manager.on_quote(&quote, 2_000_000_000).is_empty());
        assert!(!manager.check_circuit_breaker_recovery(0), "0 disables the cooldown");
        assert!(!manager.check_circuit_breaker_recovery(300), "cooldown has not elapsed");
        manager.circuit_breaker_triggered_at_ms -= 301_000;
        assert!(manager.check_circuit_breaker_recovery(300));
        assert!(!manager.check_circuit_breaker_recovery(300), "reported once per trip");
        assert!(manager.has_pause_reason(PauseReason::CircuitBreaker), "paused until reconciliation verifies");
        manager.finish_reconciliation();
        assert!(!manager.is_paused());
        assert_eq!(manager.on_quote(&quote, 3_000_000_000).len(), 2);
    }

    #[test]
    fn dispatch_failure_rolls_back_local_state() {
        let mut manager = OrderManager::new(OrderManagerConfig::default(), create_test_position(), create_test_equity_high_limit());
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);
        manager.on_dispatch_failed(&decisions[0]);
        assert!(manager.bid_order_at(0).is_none(), "a send that never left frees the slot");
        let OrderDecision::Send { cl_ord_id, .. } = &decisions[1] else { panic!("unexpected {decisions:?}") };
        assert!(manager.on_order_accepted(cl_ord_id, 9));

        let quote2 = create_test_quote(101000.0, 101100.0, 0.001);
        let decisions2 = manager.on_quote(&quote2, 2_000_000_000);
        let cancel = decisions2.iter().find(|d| matches!(d, OrderDecision::CancelAndReplace { .. })).unwrap();
        assert_eq!(manager.ask_order_at(0).unwrap().state, OrderState::Canceling);
        manager.on_dispatch_failed(cancel);
        assert_eq!(manager.ask_order_at(0).unwrap().state, OrderState::Live, "a cancel that never left reverts");
    }

    #[test]
    fn default_limit_reserves_all_levels_and_cancels_when_equity_falls() {
        let position = create_test_position();
        position.set(7.30);
        let equity = create_test_equity(738.0);
        let mut manager = OrderManager::new(OrderManagerConfig {
            num_levels: 2, lot_size: 0.01, min_order_qty: 0.01,
            min_order_qty_dollar: 1.0, ..OrderManagerConfig::default()
        }, position, Arc::clone(&equity));
        let quote = create_test_quote_2_levels(99.0, 101.0, 0.9);
        let decisions = manager.on_quote(&quote, 1_000_000_000);
        let buys: f64 = decisions.iter().filter_map(|d| match d {
            OrderDecision::Send { side: Side::Buy, qty, price, .. } => Some(qty * price.max(100.0)), _ => None,
        }).sum();
        assert!(buys > 0.0 && 730.0 + buys <= 738.0 + 1e-9);
        equity.set_equity(500.0);
        let decisions = manager.on_quote(&quote, 2_000_000_000);
        assert!(decisions.iter().any(|d| matches!(d, OrderDecision::Cancel { .. })));
        assert!(!decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Buy, .. })));
        assert!(manager.bid_order_at(0).is_some(), "canceling exposure remains reserved");
    }

    #[test]
    fn invalid_limits_and_incremental_caps_never_disable_absolute_limit() {
        let position = create_test_position();
        position.set(10.0);
        let equity = create_test_equity(500.0);
        let mut manager = OrderManager::new(OrderManagerConfig {
            position_limit_from_start: true, absolute_max_position_dollar: Some(50.0),
            ..OrderManagerConfig::default()
        }, position, Arc::clone(&equity));
        manager.initialize_position_baseline();
        let quote = create_test_quote(99.0, 101.0, 0.2);
        let orders = manager.on_quote(&quote, 1_000_000_000);
        assert!(!orders.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Buy, .. })));
        assert!(orders.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Sell, .. })));
        equity.set_equity(0.0);
        let orders = manager.on_quote(&quote, 2_000_000_000);
        assert!(orders.iter().all(|d| matches!(d, OrderDecision::Cancel { .. })));
        assert!(manager.has_pause_reason(PauseReason::RiskData));
    }

    #[test]
    fn precision_minimum_and_quantity_validation_apply_to_replacements() {
        let mut manager = OrderManager::new(OrderManagerConfig {
            min_order_qty_dollar: 1.0, lot_size: 0.01, min_order_qty: 0.01,
            ..OrderManagerConfig::default()
        }, create_test_position(), create_test_equity_high_limit());
        let quote = create_test_quote(99.0, 101.0, 0.129);
        let first = manager.on_quote(&quote, 1_000_000_000);
        for (i, d) in first.iter().enumerate() {
            if let OrderDecision::Send { cl_ord_id, qty, .. } = d {
                assert!((*qty - 0.12).abs() < 1e-12);
                manager.on_order_accepted(cl_ord_id, i as i64 + 1);
            }
        }
        manager.set_precision(0.1, 0.1, 0.2);
        let next = manager.on_quote(&quote, 2_000_000_000);
        assert_eq!(next.len(), 2);
        assert!(next.iter().all(|d| matches!(d, OrderDecision::Cancel { .. })));
        manager.on_order_canceled(1); manager.on_order_canceled(2);
        assert!(manager.on_quote(&quote, 3_000_000_000).is_empty());
        let mut invalid = quote;
        invalid.quantity = f64::NAN;
        assert!(manager.on_quote(&invalid, 4_000_000_000).is_empty());
    }

    #[test]
    fn timed_out_slots_are_retained_until_verified_reconciliation() {
        let mut manager = OrderManager::new(OrderManagerConfig::default(), create_test_position(), create_test_equity_high_limit());
        manager.on_quote(&create_test_quote(99.0, 101.0, 1.0), 1);
        assert!(!manager.check_timeouts_now(10_000_000_000).is_empty());
        assert_eq!(manager.get_all_live_order_ids().len(), 2);
        assert!(manager.has_pause_reason(PauseReason::Reconciliation));
    }
}
