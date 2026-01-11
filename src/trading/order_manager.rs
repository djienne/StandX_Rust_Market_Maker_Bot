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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::{debug, error, info, warn};

use crate::strategy::Quote;
use crate::trading::SharedPosition;

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

/// Tracked live order (one per side per symbol).
#[derive(Debug, Clone)]
pub struct LiveOrder {
    /// Client order ID (used for cancellation).
    pub cl_ord_id: String,
    /// Exchange order ID (set on acceptance).
    pub order_id: Option<i64>,
    /// Order side.
    pub side: Side,
    /// Order price.
    pub price: f64,
    /// Order quantity.
    pub quantity: f64,
    /// Current state in the state machine.
    pub state: OrderState,
    /// Timestamp when order was sent (nanoseconds).
    pub sent_at_ns: i64,
}

/// Order decision from the hot path.
#[derive(Debug, Clone)]
pub enum OrderDecision {
    /// No action needed.
    NoAction,
    /// Send a new order.
    Send {
        side: Side,
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
        new_price: f64,
        qty: f64,
    },
}

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
    /// Maximum position in dollar value (from strategy config).
    pub max_position_dollar: f64,
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
    /// Enable debug logging for order state tracking.
    pub debug: bool,
    /// Maximum consecutive rejections before pausing trading (circuit breaker).
    /// Set to 0 to disable.
    pub circuit_breaker_rejections: u32,
}

impl Default for OrderManagerConfig {
    fn default() -> Self {
        Self {
            symbol: "BTC-USD".to_string(),
            reprice_threshold_bps: 1.0,
            max_position_dollar: 500.0,
            pending_timeout_ns: 5_000_000_000, // 5 seconds
            max_live_age_ns: 60_000_000_000,   // 60 seconds max age for Live orders
            tick_size: 0.01,
            lot_size: 0.001,
            debug: false,
            circuit_breaker_rejections: 5,     // Pause after 5 consecutive rejections
        }
    }
}

/// Timeout check interval in nanoseconds (1 second).
/// This limits how often we check for timed out orders to minimize hot path latency.
const TIMEOUT_CHECK_INTERVAL_NS: i64 = 1_000_000_000;

/// Low-latency order manager for market making.
///
/// Provides synchronous order decision-making in the hot path with
/// fire-and-forget async execution.
pub struct OrderManager {
    /// Configuration.
    config: OrderManagerConfig,

    /// Current bid order (buy).
    bid_order: Option<LiveOrder>,

    /// Current ask order (sell).
    ask_order: Option<LiveOrder>,

    /// Shared position from poller (lock-free reads).
    position: Arc<SharedPosition>,

    /// Client order ID counter.
    cl_ord_id_counter: AtomicU64,

    /// Session prefix for client order IDs.
    session_prefix: String,

    /// Statistics.
    stats: OrderManagerStats,

    /// Shutdown flag.
    shutdown: AtomicBool,

    /// Paused flag (for WebSocket disconnection).
    /// When paused, no new orders are generated.
    paused: AtomicBool,

    /// Last time we checked for timeouts (throttles hot path).
    last_timeout_check_ns: i64,

    /// Pending bid price after cancel confirms (for CancelAndReplace).
    /// Stores (price, qty) to place immediately when slot is cleared.
    pending_bid_price: Option<(f64, f64)>,

    /// Pending ask price after cancel confirms (for CancelAndReplace).
    /// Stores (price, qty) to place immediately when slot is cleared.
    pending_ask_price: Option<(f64, f64)>,

    /// Consecutive rejection count for circuit breaker.
    consecutive_rejections: u32,
}

impl OrderManager {
    /// Create a new order manager.
    pub fn new(config: OrderManagerConfig, position: Arc<SharedPosition>) -> Self {
        // Generate session prefix including symbol for O(1) lookup by symbol in event handlers
        // Format: mm_<symbol>_<timestamp>
        let session_prefix = format!("mm_{}_{}", config.symbol, chrono::Utc::now().timestamp_millis() % 1_000_000);

        Self {
            config,
            bid_order: None,
            ask_order: None,
            position,
            cl_ord_id_counter: AtomicU64::new(1),
            session_prefix,
            stats: OrderManagerStats::default(),
            shutdown: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            last_timeout_check_ns: 0,
            pending_bid_price: None,
            pending_ask_price: None,
            consecutive_rejections: 0,
        }
    }

    /// Get the symbol being managed.
    pub fn symbol(&self) -> &str {
        &self.config.symbol
    }

    /// Get current statistics.
    pub fn stats(&self) -> &OrderManagerStats {
        &self.stats
    }

    /// Generate a unique client order ID.
    /// Format: mm_<symbol>_<timestamp>_<seq>
    #[inline]
    pub fn generate_cl_ord_id(&self) -> String {
        let seq = self.cl_ord_id_counter.fetch_add(1, Ordering::Relaxed);
        format!("{}_{}", self.session_prefix, seq)
    }

    /// Extract symbol from a client order ID.
    /// Format: mm_<symbol>_<timestamp>_<seq>
    /// Returns None if format doesn't match.
    #[inline]
    pub fn extract_symbol_from_cl_ord_id(cl_ord_id: &str) -> Option<&str> {
        // Format: mm_<symbol>_<timestamp>_<seq>
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
        // Early exit if shutting down or paused (atomic, no latency)
        // Use Acquire ordering to ensure we see the Release store from shutdown()/pause()
        if self.shutdown.load(Ordering::Acquire) || self.paused.load(Ordering::Acquire) {
            return vec![];
        }

        // Early exit if quote not valid for trading
        if !quote.valid_for_trading {
            return vec![];
        }

        // Validate mid_price to prevent NaN/Inf from bypassing position limits
        // (NaN comparisons always return false, which would skip all limit checks)
        if !quote.mid_price.is_finite() || quote.mid_price <= 0.0 {
            return vec![];
        }

        // Calculate position in dollars (from poller - source of truth)
        let position_dollar = self.position.get() * quote.mid_price;
        // Use > and < (not >= and <=) so that at exactly the limit we can still
        // place orders on the opposite side to rebalance position
        let at_max_long = position_dollar > self.config.max_position_dollar;
        let at_max_short = position_dollar < -self.config.max_position_dollar;

        let mut decisions = Vec::with_capacity(4);

        // Process BID side (buy) - skip if at max long position
        if !at_max_long {
            if let Some(decision) = self.process_side(
                Side::Buy,
                quote.bid_price,
                quote.quantity,
                current_time_ns,
            ) {
                decisions.push(decision);
            }
        } else if let Some(bid) = &self.bid_order {
            // At max long - cancel any existing bid
            if bid.state != OrderState::Canceling {
                debug!(
                    "[{}] At max long position ({:.2} > {:.2}), canceling bid",
                    self.config.symbol, position_dollar, self.config.max_position_dollar
                );
                decisions.push(OrderDecision::Cancel {
                    cl_ord_id: bid.cl_ord_id.clone(),
                });
            }
        }

        // Process ASK side (sell) - skip if at max short position
        if !at_max_short {
            if let Some(decision) = self.process_side(
                Side::Sell,
                quote.ask_price,
                quote.quantity,
                current_time_ns,
            ) {
                decisions.push(decision);
            }
        } else if let Some(ask) = &self.ask_order {
            // At max short - cancel any existing ask
            if ask.state != OrderState::Canceling {
                debug!(
                    "[{}] At max short position ({:.2} < -{:.2}), canceling ask",
                    self.config.symbol, position_dollar, self.config.max_position_dollar
                );
                decisions.push(OrderDecision::Cancel {
                    cl_ord_id: ask.cl_ord_id.clone(),
                });
            }
        }

        decisions
    }

    /// Process one side (bid or ask) and return decision.
    #[inline]
    fn process_side(
        &mut self,
        side: Side,
        new_price: f64,
        qty: f64,
        current_time_ns: i64,
    ) -> Option<OrderDecision> {
        // Check order state without cloning - only clone cl_ord_id when needed
        let order = match side {
            Side::Buy => self.bid_order.as_ref(),
            Side::Sell => self.ask_order.as_ref(),
        };

        match order {
            None => {
                // No order on this side - place new one
                let cl_ord_id = self.generate_cl_ord_id();
                debug!(
                    "[{}] NEW {} order: price={:.2}, qty={:.6}, id={}",
                    self.config.symbol, side, new_price, qty, cl_ord_id
                );
                self.set_order_pending(side, cl_ord_id.clone(), new_price, qty, current_time_ns);
                self.stats.orders_sent += 1;
                Some(OrderDecision::Send {
                    side,
                    price: new_price,
                    qty,
                    cl_ord_id,
                })
            }
            Some(o) if o.state == OrderState::Pending => {
                // Still pending - wait for confirmation
                let age_secs = (current_time_ns - o.sent_at_ns) / 1_000_000_000;
                if self.config.debug {
                    info!(
                        "[{}] {} order blocked: pending confirmation for {} ({}s ago)",
                        self.config.symbol, side, o.cl_ord_id, age_secs
                    );
                }
                None
            }
            Some(o) if o.state == OrderState::Canceling => {
                // Cancel in progress - wait for confirmation
                let age_secs = (current_time_ns - o.sent_at_ns) / 1_000_000_000;
                // Always log at info level for Canceling - this blocks new orders
                info!(
                    "[{}] {} order blocked: cancel pending for {} ({}s ago, order_id={:?})",
                    self.config.symbol, side, o.cl_ord_id, age_secs, o.order_id
                );
                None
            }
            Some(o) => {
                // Live order - check if reprice needed
                let price_changed = self.should_reprice(o.price, new_price);
                let age_ns = current_time_ns - o.sent_at_ns;
                let order_too_old = self.config.max_live_age_ns > 0
                    && age_ns > self.config.max_live_age_ns as i64;

                if price_changed || order_too_old {
                    let change_bps = ((new_price - o.price) / o.price).abs() * 10_000.0;
                    let age_secs = age_ns / 1_000_000_000;

                    if order_too_old && !price_changed {
                        debug!(
                            "[{}] REFRESH {} order (age={}s > {}s): {:.2} -> {:.2} ({:.1}bps), id={}",
                            self.config.symbol, side, age_secs,
                            self.config.max_live_age_ns / 1_000_000_000,
                            o.price, new_price, change_bps, o.cl_ord_id
                        );
                    } else {
                        debug!(
                            "[{}] REPRICE {} order: {:.2} -> {:.2} ({:.1}bps), id={}",
                            self.config.symbol, side, o.price, new_price, change_bps, o.cl_ord_id
                        );
                    }
                    let cancel_id = o.cl_ord_id.clone(); // Only clone when actually repricing
                    self.set_order_canceling(side, current_time_ns);
                    self.stats.reprices += 1;

                    // Store pending price for immediate placement after cancel confirms
                    match side {
                        Side::Buy => self.pending_bid_price = Some((new_price, qty)),
                        Side::Sell => self.pending_ask_price = Some((new_price, qty)),
                    }

                    Some(OrderDecision::CancelAndReplace {
                        cancel_id,
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
        let mut cancels = Vec::new();
        let timeout_ns = self.config.pending_timeout_ns as i64;
        let timeout_secs = timeout_ns / 1_000_000_000;

        // Check bid order - only timeout Pending or Canceling orders
        // Live orders should NOT timeout - they're valid on the exchange
        if let Some(order) = &self.bid_order {
            // Skip Live orders - they don't need timeout checking
            if order.state == OrderState::Live {
                // Live order is fine, no timeout needed
            } else {
                let age_ns = current_time_ns - order.sent_at_ns;
                let age_secs = age_ns / 1_000_000_000;

                // Debug: log every check when order exists and is getting old
                if age_secs >= 50 {
                    info!(
                        "[{}] Timeout check: {} order {} state={:?} age={}s timeout={}s",
                        self.config.symbol, order.side, order.cl_ord_id, order.state, age_secs, timeout_secs
                    );
                }

                if age_ns > timeout_ns {
                    warn!(
                        "[{}] {} order {} ({:?}) timed out after {}s (>{}s) - CLEARING SLOT",
                        self.config.symbol, order.side, order.cl_ord_id, order.state, age_secs, timeout_secs
                    );
                    cancels.push(OrderDecision::Cancel {
                        cl_ord_id: order.cl_ord_id.clone(),
                    });
                    self.stats.timeouts += 1;
                    // Clear immediately
                    self.bid_order = None;
                }
            }
        }

        // Check ask order - only timeout Pending or Canceling orders
        // Live orders should NOT timeout - they're valid on the exchange
        if let Some(order) = &self.ask_order {
            // Skip Live orders - they don't need timeout checking
            if order.state == OrderState::Live {
                // Live order is fine, no timeout needed
            } else {
                let age_ns = current_time_ns - order.sent_at_ns;
                let age_secs = age_ns / 1_000_000_000;

                // Debug: log every check when order exists and is getting old
                if age_secs >= 50 {
                    info!(
                        "[{}] Timeout check: {} order {} state={:?} age={}s timeout={}s",
                        self.config.symbol, order.side, order.cl_ord_id, order.state, age_secs, timeout_secs
                    );
                }

                if age_ns > timeout_ns {
                    warn!(
                        "[{}] {} order {} ({:?}) timed out after {}s (>{}s) - CLEARING SLOT",
                        self.config.symbol, order.side, order.cl_ord_id, order.state, age_secs, timeout_secs
                    );
                    cancels.push(OrderDecision::Cancel {
                        cl_ord_id: order.cl_ord_id.clone(),
                    });
                    self.stats.timeouts += 1;
                    // Clear immediately
                    self.ask_order = None;
                }
            }
        }

        cancels
    }

    /// Set order state to pending.
    fn set_order_pending(
        &mut self,
        side: Side,
        cl_ord_id: String,
        price: f64,
        quantity: f64,
        sent_at_ns: i64,
    ) {
        let order = LiveOrder {
            cl_ord_id,
            order_id: None,
            side,
            price,
            quantity,
            state: OrderState::Pending,
            sent_at_ns,
        };

        match side {
            Side::Buy => self.bid_order = Some(order),
            Side::Sell => self.ask_order = Some(order),
        }
    }

    /// Set order state to canceling and reset the timeout clock.
    ///
    /// Updating `sent_at_ns` when entering Canceling state ensures that:
    /// 1. Timeout is measured from when cancel was initiated, not original order placement
    /// 2. If cancel fails and we revert to Live, the timeout won't fire prematurely
    fn set_order_canceling(&mut self, side: Side, current_time_ns: i64) {
        let order = match side {
            Side::Buy => &mut self.bid_order,
            Side::Sell => &mut self.ask_order,
        };

        if let Some(o) = order {
            o.state = OrderState::Canceling;
            o.sent_at_ns = current_time_ns; // Reset timeout clock for cancel operation
        }
    }

    // ========== Event Handlers (called from async context) ==========

    /// Called when an order is accepted by the exchange.
    pub fn on_order_accepted(&mut self, cl_ord_id: &str, order_id: i64) {
        // Reset circuit breaker on successful acceptance
        self.consecutive_rejections = 0;

        if let Some(order) = self.find_order_mut(cl_ord_id) {
            let side = order.side;
            let prev_state = order.state;
            let sent_at_ns = order.sent_at_ns;

            // Always update order_id (needed for cancel matching)
            order.order_id = Some(order_id);

            // Only transition to Live if still Pending
            // If already Canceling, keep it Canceling (cancel is in flight)
            if prev_state == OrderState::Pending {
                order.state = OrderState::Live;
                self.stats.orders_accepted += 1;

                // Calculate acceptance latency
                use std::time::{SystemTime, UNIX_EPOCH};
                let now_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64;
                let latency_ms = (now_ns - sent_at_ns) / 1_000_000;

                debug!(
                    "[{}] {} order accepted: {} -> {} (latency={}ms)",
                    self.config.symbol, side, cl_ord_id, order_id, latency_ms
                );
            } else if prev_state == OrderState::Canceling {
                // Order was already being canceled when acceptance arrived
                // Keep in Canceling state - cancel request is in flight
                warn!(
                    "[{}] {} order {} accepted while Canceling (order_id={}) - keeping Canceling state",
                    self.config.symbol, side, cl_ord_id, order_id
                );
                self.stats.orders_accepted += 1;
            }
        }
    }

    /// Called when an order is rejected.
    pub fn on_order_rejected(&mut self, cl_ord_id: &str, reason: &str) {
        // Get side before clearing
        let side = self.find_order(cl_ord_id).map(|o| o.side);
        let side_str = side.map(|s| format!("{} ", s)).unwrap_or_default();
        warn!(
            "[{}] {}order rejected: {} - {}",
            self.config.symbol, side_str, cl_ord_id, reason
        );
        self.clear_order_by_cl_ord_id(cl_ord_id);
        self.stats.rejections += 1;

        // Also clear pending price for this side to avoid placing stale orders
        if let Some(s) = side {
            self.clear_pending_price(s);
        }

        // Circuit breaker: pause trading after too many consecutive rejections
        self.consecutive_rejections += 1;
        if self.config.circuit_breaker_rejections > 0
            && self.consecutive_rejections >= self.config.circuit_breaker_rejections
        {
            error!(
                "[{}] CIRCUIT BREAKER: {} consecutive rejections - PAUSING trading",
                self.config.symbol, self.consecutive_rejections
            );
            self.paused.store(true, Ordering::Release);
        }
    }

    /// Reset circuit breaker and resume trading.
    pub fn reset_circuit_breaker(&mut self) {
        self.consecutive_rejections = 0;
        self.paused.store(false, Ordering::Release);
        info!("[{}] Circuit breaker reset, trading resumed", self.config.symbol);
    }

    /// Get consecutive rejection count.
    pub fn consecutive_rejections(&self) -> u32 {
        self.consecutive_rejections
    }

    /// Called when an order is canceled.
    pub fn on_order_canceled(&mut self, order_id: i64) {
        // Get info for logging before clearing
        let order_info = self.bid_order.as_ref()
            .filter(|o| o.order_id == Some(order_id))
            .map(|o| (o.cl_ord_id.clone(), o.side, o.state))
            .or_else(|| {
                self.ask_order.as_ref()
                    .filter(|o| o.order_id == Some(order_id))
                    .map(|o| (o.cl_ord_id.clone(), o.side, o.state))
            });

        if let Some((cl_ord_id, side, state)) = order_info {
            info!(
                "[{}] {} order canceled: {} (order_id={}, was {:?}) - slot freed",
                self.config.symbol, side, cl_ord_id, order_id, state
            );
        } else {
            // Order not found - may have been force-cleared or already canceled
            debug!(
                "[{}] Cancel confirmation for unknown order_id={} - already cleared?",
                self.config.symbol, order_id
            );
        }
        self.clear_order_by_exchange_id(order_id);
        self.stats.orders_canceled += 1;
    }

    /// Called when an order cancel is confirmed by client order ID.
    pub fn on_order_canceled_by_cl_ord_id(&mut self, cl_ord_id: &str) {
        // Get info for logging before clearing
        let order_info = self.find_order(cl_ord_id).map(|o| (o.side, o.state));

        if let Some((side, state)) = order_info {
            info!(
                "[{}] {} order canceled: {} (was {:?}) - slot freed",
                self.config.symbol, side, cl_ord_id, state
            );
        } else {
            debug!(
                "[{}] Cancel confirmation for unknown cl_ord_id={} - already cleared?",
                self.config.symbol, cl_ord_id
            );
        }
        self.clear_order_by_cl_ord_id(cl_ord_id);
        self.stats.orders_canceled += 1;
    }

    /// Called when a cancel request fails.
    ///
    /// The order is still live on the exchange, so we revert from Canceling
    /// back to Live state to allow repricing on the next quote.
    pub fn on_cancel_failed(&mut self, order_id: i64, reason: &str) {
        warn!(
            "[{}] Cancel failed for order {}: {}",
            self.config.symbol, order_id, reason
        );

        // Find the order and revert to Live state
        if let Some(order) = &mut self.bid_order {
            if order.order_id == Some(order_id) && order.state == OrderState::Canceling {
                order.state = OrderState::Live;
                debug!("[{}] Reverted bid order {} to Live state", self.config.symbol, order_id);
                return;
            }
        }
        if let Some(order) = &mut self.ask_order {
            if order.order_id == Some(order_id) && order.state == OrderState::Canceling {
                order.state = OrderState::Live;
                debug!("[{}] Reverted ask order {} to Live state", self.config.symbol, order_id);
            }
        }
    }

    // ========== Pending Order Handling ==========

    /// Check if we have pending prices to place after a slot was cleared.
    ///
    /// This implements immediate order placement after cancel confirms,
    /// avoiding the race condition where price changes during cancel wait.
    /// Returns decisions for immediate order placement.
    pub fn check_pending_orders(&mut self, current_time_ns: i64) -> Vec<OrderDecision> {
        let mut decisions = Vec::new();

        // Check bid - only place if slot is empty
        if self.bid_order.is_none() {
            if let Some((price, qty)) = self.pending_bid_price.take() {
                let cl_ord_id = self.generate_cl_ord_id();
                debug!(
                    "[{}] Placing pending BID: {:.2} x {:.6}, id={}",
                    self.config.symbol, price, qty, cl_ord_id
                );
                self.set_order_pending(Side::Buy, cl_ord_id.clone(), price, qty, current_time_ns);
                self.stats.orders_sent += 1;
                decisions.push(OrderDecision::Send {
                    side: Side::Buy,
                    price,
                    qty,
                    cl_ord_id,
                });
            }
        }

        // Check ask - only place if slot is empty
        if self.ask_order.is_none() {
            if let Some((price, qty)) = self.pending_ask_price.take() {
                let cl_ord_id = self.generate_cl_ord_id();
                debug!(
                    "[{}] Placing pending ASK: {:.2} x {:.6}, id={}",
                    self.config.symbol, price, qty, cl_ord_id
                );
                self.set_order_pending(Side::Sell, cl_ord_id.clone(), price, qty, current_time_ns);
                self.stats.orders_sent += 1;
                decisions.push(OrderDecision::Send {
                    side: Side::Sell,
                    price,
                    qty,
                    cl_ord_id,
                });
            }
        }

        decisions
    }

    /// Clear pending price for a side (e.g., on rejection or timeout).
    pub fn clear_pending_price(&mut self, side: Side) {
        match side {
            Side::Buy => self.pending_bid_price = None,
            Side::Sell => self.pending_ask_price = None,
        }
    }

    /// Clear all pending prices (e.g., on disconnect).
    pub fn clear_all_pending_prices(&mut self) {
        self.pending_bid_price = None;
        self.pending_ask_price = None;
    }

    // ========== Order Lookup Helpers ==========

    #[inline]
    fn find_order(&self, cl_ord_id: &str) -> Option<&LiveOrder> {
        if let Some(order) = &self.bid_order {
            if order.cl_ord_id == cl_ord_id {
                return Some(order);
            }
        }
        if let Some(order) = &self.ask_order {
            if order.cl_ord_id == cl_ord_id {
                return Some(order);
            }
        }
        None
    }

    #[inline]
    fn find_order_mut(&mut self, cl_ord_id: &str) -> Option<&mut LiveOrder> {
        if let Some(order) = &mut self.bid_order {
            if order.cl_ord_id == cl_ord_id {
                return Some(order);
            }
        }
        if let Some(order) = &mut self.ask_order {
            if order.cl_ord_id == cl_ord_id {
                return Some(order);
            }
        }
        None
    }

    #[inline]
    fn clear_order_by_cl_ord_id(&mut self, cl_ord_id: &str) {
        if let Some(order) = &self.bid_order {
            if order.cl_ord_id == cl_ord_id {
                self.bid_order = None;
                return;
            }
        }
        if let Some(order) = &self.ask_order {
            if order.cl_ord_id == cl_ord_id {
                self.ask_order = None;
            }
        }
    }

    #[inline]
    fn clear_order_by_exchange_id(&mut self, order_id: i64) {
        if let Some(order) = &self.bid_order {
            if order.order_id == Some(order_id) {
                self.bid_order = None;
                return;
            }
        }
        if let Some(order) = &self.ask_order {
            if order.order_id == Some(order_id) {
                self.ask_order = None;
            }
        }
    }

    // ========== Pause/Resume (for WebSocket disconnection) ==========

    /// Pause trading (stop generating new orders).
    /// Called when Order WebSocket disconnects.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Release);
        info!("[{}] Order manager paused (WebSocket disconnected)", self.config.symbol);
    }

    /// Resume trading after WebSocket reconnection.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Release);
        info!("[{}] Order manager resumed (WebSocket reconnected)", self.config.symbol);
    }

    /// Check if trading is paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    // ========== Shutdown ==========

    /// Initiate graceful shutdown - stop placing new orders.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        info!("[{}] Order manager shutdown initiated", self.config.symbol);
    }

    /// Check if shutdown is in progress.
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Get all live order client IDs for batch cancel on shutdown.
    pub fn get_all_live_order_ids(&self) -> Vec<String> {
        let mut ids = Vec::with_capacity(2);

        if let Some(order) = &self.bid_order {
            if matches!(order.state, OrderState::Live | OrderState::Pending) {
                ids.push(order.cl_ord_id.clone());
            }
        }
        if let Some(order) = &self.ask_order {
            if matches!(order.state, OrderState::Live | OrderState::Pending) {
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
        if self.bid_order.is_some() {
            debug!("[{}] Clearing bid order from state", self.config.symbol);
            self.bid_order = None;
        }
        if self.ask_order.is_some() {
            debug!("[{}] Clearing ask order from state", self.config.symbol);
            self.ask_order = None;
        }
    }

    /// Get current bid order info (for logging/monitoring).
    pub fn bid_order(&self) -> Option<&LiveOrder> {
        self.bid_order.as_ref()
    }

    /// Get current ask order info (for logging/monitoring).
    pub fn ask_order(&self) -> Option<&LiveOrder> {
        self.ask_order.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_position() -> Arc<SharedPosition> {
        Arc::new(SharedPosition::new("BTC-USD".to_string()))
    }

    fn create_test_quote(bid: f64, ask: f64, qty: f64) -> Quote {
        Quote {
            symbol: "BTC-USD".into(),
            bid_price: bid,
            ask_price: ask,
            quantity: qty,
            mid_price: (bid + ask) / 2.0,
            spread: ask - bid,
            volatility: 0.001,
            alpha: 0.0,
            position: 0.0,
            half_spread_tick: 1.0,
            valid_for_trading: true,
            history_secs: 600.0,
            bid_floored: false,
            ask_floored: false,
        }
    }

    #[test]
    fn test_new_order_manager() {
        let position = create_test_position();
        let config = OrderManagerConfig::default();
        let manager = OrderManager::new(config, position);

        assert!(manager.bid_order().is_none());
        assert!(manager.ask_order().is_none());
        assert_eq!(manager.stats().orders_sent, 0);
    }

    #[test]
    fn test_initial_order_placement() {
        let position = create_test_position();
        let config = OrderManagerConfig::default();
        let mut manager = OrderManager::new(config, position);

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
    fn test_no_reprice_within_threshold() {
        let position = create_test_position();
        let mut config = OrderManagerConfig::default();
        config.reprice_threshold_bps = 10.0; // 10 bps threshold
        let mut manager = OrderManager::new(config, position);

        // Place initial orders
        let quote = create_test_quote(100000.0, 100010.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);
        assert_eq!(decisions.len(), 2);

        // Simulate acceptance
        if let Some(OrderDecision::Send { cl_ord_id, .. }) = decisions.get(0) {
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
        let mut config = OrderManagerConfig::default();
        config.reprice_threshold_bps = 1.0; // 1 bps threshold
        let mut manager = OrderManager::new(config, position);

        // Place initial orders
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);

        // Simulate acceptance
        for decision in &decisions {
            if let OrderDecision::Send { cl_ord_id, .. } = decision {
                manager.on_order_accepted(cl_ord_id, 1001);
            }
        }

        // Large price change (100 bps)
        let quote2 = create_test_quote(101000.0, 101100.0, 0.001);
        let decisions2 = manager.on_quote(&quote2, 2_000_000_000);

        // Should reprice
        assert!(!decisions2.is_empty(), "Should reprice beyond threshold");
    }

    #[test]
    fn test_position_limit_long() {
        let position = create_test_position();
        position.set(0.005); // 0.005 BTC position

        let mut config = OrderManagerConfig::default();
        config.max_position_dollar = 500.0; // $500 max
        let mut manager = OrderManager::new(config, position);

        // Quote at $100,000 means position = $500 (at max)
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);

        // Should only place ask (sell), not bid (buy) since at max long
        assert!(decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Sell, .. })));
        assert!(!decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Buy, .. })));
    }

    #[test]
    fn test_shutdown() {
        let position = create_test_position();
        let config = OrderManagerConfig::default();
        let mut manager = OrderManager::new(config, position);

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
}
