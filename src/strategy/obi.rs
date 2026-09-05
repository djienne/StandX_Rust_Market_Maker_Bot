//! OBI (Order Book Imbalance) market making strategy.
//!
//! Calculates bid/ask quotes based on:
//! - Volatility of mid-price changes
//! - Z-score of order book imbalance (alpha)
//! - Position skew adjustment

use std::sync::Arc;
use tracing::{debug, info};
use crate::binance::SharedAlpha;
use crate::config::StrategyConfig;
use crate::trading::{SharedEquity, SharedSymbolInfo};
use crate::types::OrderbookSnapshot;
use super::rolling::RollingStats;
use super::quotes::Quote;
use super::traits::QuoteStrategy;

/// OBI market making strategy.
pub struct ObiStrategy {
    /// Strategy configuration
    config: StrategyConfig,
    /// Shared symbol info for dynamic tick_size/lot_size (optional)
    shared_info: Option<Arc<SharedSymbolInfo>>,
    /// Shared equity for automatic order sizing (optional)
    shared_equity: Option<Arc<SharedEquity>>,
    /// Shared Binance alpha for lock-free reads (optional)
    shared_binance_alpha: Option<Arc<SharedAlpha>>,
    /// Whether to use Binance alpha (from config)
    use_binance_alpha: bool,
    /// Binance stale threshold in milliseconds
    binance_stale_ms: u64,
    /// Rolling window for mid-price changes (volatility)
    mid_price_chg_stats: RollingStats,
    /// Rolling window for imbalance (alpha)
    imbalance_stats: RollingStats,
    /// Previous mid-price in dollars (price units)
    prev_mid_price: Option<f64>,
    /// Current position in base asset units
    position: f64,
    /// Current step count
    step_count: u64,
    /// Last update step
    last_update_step: u64,
    /// Cached volatility (price per square-root second)
    volatility: f64,
    /// Cached alpha (z-score of imbalance)
    alpha: f64,
    /// Is strategy warmed up (enough samples for quoting)
    warmed_up: bool,
    /// First timestamp seen (for tracking history age)
    first_timestamp_ns: Option<i64>,
    /// Most recent timestamp
    latest_timestamp_ns: i64,
    /// Required history duration for trading (default: 10 minutes in ns)
    required_history_ns: u64,
    /// Total samples pushed to mid_price_chg_stats (not capped by window size)
    total_samples: usize,
    /// Has logged the "valid for trading" milestone (log only once)
    logged_valid_for_trading: bool,
    /// Has logged the "using Binance alpha" milestone (log only once)
    logged_binance_alpha_active: bool,
    held_observation: Option<(f64, f64)>,
    next_sample_ns: i64,
    last_observation_ns: i64,
    max_gap_ns: i64,
}

/// Minimum grid samples needed before generating quotes.
/// The actual trading wait time is controlled by `history_minutes` in config.
const MIN_SAMPLES_FOR_QUOTE: usize = 100;

impl ObiStrategy {
    pub fn new(
        config: StrategyConfig,
        shared_info: Option<Arc<SharedSymbolInfo>>,
        shared_equity: Option<Arc<SharedEquity>>,
        shared_binance_alpha: Option<Arc<SharedAlpha>>,
        required_history_minutes: u64,
        depth_stale_secs: u64,
    ) -> Self {
        let window_steps = config.window_steps;
        Self {
            use_binance_alpha: config.alpha_source == "binance",
            binance_stale_ms: config.binance_stale_ms,
            config, shared_info, shared_equity, shared_binance_alpha,
            mid_price_chg_stats: RollingStats::new(window_steps),
            imbalance_stats: RollingStats::new(window_steps),
            prev_mid_price: None,
            position: 0.0,
            step_count: 0,
            last_update_step: 0,
            volatility: 0.0,
            alpha: 0.0,
            warmed_up: false,
            first_timestamp_ns: None,
            latest_timestamp_ns: 0,
            required_history_ns: required_history_minutes * 60 * 1_000_000_000,
            total_samples: 0,
            logged_valid_for_trading: false,
            logged_binance_alpha_active: false,
            held_observation: None,
            next_sample_ns: 0,
            last_observation_ns: 0,
            max_gap_ns: depth_stale_secs as i64 * 1_000_000_000,
        }
    }

    /// Get the current tick size (from SharedSymbolInfo if available, else from config).
    #[inline]
    pub fn tick_size(&self) -> f64 {
        self.shared_info
            .as_ref()
            .map(|info| info.tick_size())
            .unwrap_or(self.config.tick_size)
    }

    /// Get the current lot size (from SharedSymbolInfo if available, else from config).
    #[inline]
    pub fn lot_size(&self) -> f64 {
        self.shared_info
            .as_ref()
            .map(|info| info.lot_size())
            .unwrap_or(self.config.lot_size)
    }

    /// Set the current position.
    ///
    /// Position is in base asset units (e.g., BTC).
    /// Positive = long, negative = short.
    pub fn set_position(&mut self, position: f64) {
        self.position = position;
    }

    /// Get the current position.
    #[inline]
    pub fn position(&self) -> f64 {
        self.position
    }

    /// Check if the strategy is warmed up (has enough data).
    #[inline]
    pub fn is_warmed_up(&self) -> bool {
        self.warmed_up
    }

    /// Get the current volatility.
    #[inline]
    pub fn volatility(&self) -> f64 {
        self.volatility
    }

    /// Get the current alpha (imbalance z-score).
    #[inline]
    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    /// Get the strategy configuration.
    #[inline]
    pub fn config(&self) -> &StrategyConfig {
        &self.config
    }

    /// Check if the strategy has enough history for trading.
    ///
    /// Trading requires the full history window (default 10 minutes).
    /// This is separate from `is_warmed_up()` which only requires
    /// enough samples for quote calculation.
    #[inline]
    pub fn is_valid_for_trading(&self) -> bool {
        if !self.warmed_up {
            return false;
        }
        self.history_duration_ns() >= self.required_history_ns
    }

    /// Get the current history duration in nanoseconds.
    #[inline]
    pub fn history_duration_ns(&self) -> u64 {
        match self.first_timestamp_ns {
            Some(first) if self.latest_timestamp_ns > first => {
                (self.latest_timestamp_ns - first) as u64
            }
            _ => 0,
        }
    }

    /// Get the current history duration in seconds.
    #[inline]
    pub fn history_duration_secs(&self) -> f64 {
        self.history_duration_ns() as f64 / 1_000_000_000.0
    }

    /// Get the required history duration in seconds.
    #[inline]
    pub fn required_history_secs(&self) -> f64 {
        self.required_history_ns as f64 / 1_000_000_000.0
    }

    /// Process a new orderbook snapshot.
    ///
    /// Returns a Quote if the strategy is warmed up and ready to quote.
    /// Note: Quotes are returned for display purposes even before
    /// `is_valid_for_trading()` returns true. Use `is_valid_for_trading()`
    /// to check if the quote should be used for order placement.
    pub fn update(&mut self, snapshot: &OrderbookSnapshot) -> Option<Quote> {
        if snapshot.validate().is_err() { return None; }
        let timestamp = snapshot.received_at_ns;
        if timestamp < self.last_observation_ns { return None; }
        let mid_price = snapshot.mid_price()?;
        let imbalance = self.calculate_imbalance(snapshot, mid_price);
        if self.held_observation.is_some() && timestamp - self.last_observation_ns >= self.max_gap_ns {
            self.reset_state();
        }
        if let Some((mid, obi)) = self.held_observation {
            // Sample held observations strictly before arrival. A new message
            // may be used at its exact boundary, never at an earlier boundary.
            while self.next_sample_ns < timestamp {
                self.sample(self.next_sample_ns, mid, obi);
                self.next_sample_ns += self.config.step_ns as i64;
            }
            if self.next_sample_ns == timestamp {
                self.sample(timestamp, mid_price, imbalance);
                self.next_sample_ns += self.config.step_ns as i64;
            }
        } else {
            self.sample(timestamp, mid_price, imbalance);
            self.next_sample_ns = timestamp + self.config.step_ns as i64;
        }
        self.held_observation = Some((mid_price, imbalance));
        self.last_observation_ns = timestamp;

        // Check if we should update (based on update_interval_steps)
        let steps_since_update = self.step_count - self.last_update_step;
        if steps_since_update < self.config.update_interval_steps as u64 {
            return None;
        }
        self.last_update_step = self.step_count;

        // Check warm-up: need minimum samples for statistical validity
        // Note: window_steps is for rolling stats size, NOT warmup time
        // Trading readiness is controlled by history_minutes via is_valid_for_trading()
        // Use total_samples (not rolling window len) because window is capped at window_steps
        if self.total_samples < MIN_SAMPLES_FOR_QUOTE {
            return None;
        }

        // Mark as warmed up (log first time)
        if !self.warmed_up {
            debug!(
                "[{}] Strategy warmed up: {} samples, ready to generate quotes",
                snapshot.symbol, self.total_samples
            );
        }
        self.warmed_up = true;

        // Scale grid-increment volatility to price per square-root second.
        let vol_raw = self.mid_price_chg_stats.std();
        self.volatility = vol_raw * self.config.vol_scale();

        // Calculate alpha - prefer Binance if configured and available (~5ns decision)
        // This is the critical hot path optimization: lock-free atomic reads
        self.alpha = if self.use_binance_alpha {
            if let Some(ref binance) = self.shared_binance_alpha {
                if binance.is_warmed_up() && !binance.is_stale(self.binance_stale_ms) {
                    // Use Binance alpha (lock-free read, ~1ns)
                    let binance_alpha = binance.alpha();

                    // Log milestone once when Binance alpha becomes active
                    if !self.logged_binance_alpha_active {
                        info!(
                            "[{}] Using Binance alpha: {:.3} (samples={})",
                            snapshot.symbol,
                            binance_alpha,
                            binance.sample_count()
                        );
                        self.logged_binance_alpha_active = true;
                    }

                    binance_alpha
                } else {
                    // Fallback to StandX alpha (Binance not ready or stale)
                    self.imbalance_stats.zscore(imbalance)
                }
            } else {
                // No Binance alpha provided, use StandX
                self.imbalance_stats.zscore(imbalance)
            }
        } else {
            // StandX alpha explicitly configured
            self.imbalance_stats.zscore(imbalance)
        };

        // Calculate and return quote
        self.calculate_quote(snapshot, mid_price)
    }

    fn sample(&mut self, time_ns: i64, mid: f64, imbalance: f64) {
        self.first_timestamp_ns.get_or_insert(time_ns);
        self.latest_timestamp_ns = time_ns;
        if let Some(previous) = self.prev_mid_price {
            self.mid_price_chg_stats.push(mid - previous);
            self.total_samples += 1;
        }
        self.prev_mid_price = Some(mid);
        self.imbalance_stats.push(imbalance);
        self.step_count += 1;
    }

    /// Calculate order book imbalance within looking_depth of mid-price.
    /// Uses simple loops instead of iterator chains for better hot path performance.
    #[inline]
    fn calculate_imbalance(&self, snapshot: &OrderbookSnapshot, mid_price: f64) -> f64 {
        let depth_pct = self.config.looking_depth;
        let lower_bound = mid_price * (1.0 - depth_pct);
        let upper_bound = mid_price * (1.0 + depth_pct);

        // Sum bid quantities within range (bids are sorted descending by price)
        let mut sum_bid_qty = 0.0;
        for level in &snapshot.bids[..snapshot.bid_count as usize] {
            if level.price < lower_bound {
                break;
            }
            sum_bid_qty += level.quantity;
        }

        // Sum ask quantities within range (asks are sorted ascending by price)
        let mut sum_ask_qty = 0.0;
        for level in &snapshot.asks[..snapshot.ask_count as usize] {
            if level.price > upper_bound {
                break;
            }
            sum_ask_qty += level.quantity;
        }

        sum_bid_qty - sum_ask_qty
    }

    /// Calculate bid/ask quotes for multiple levels.
    #[inline]
    fn calculate_quote(&mut self, snapshot: &OrderbookSnapshot, mid_price: f64) -> Option<Quote> {
        use super::quotes::MAX_ORDER_LEVELS;

        let best_bid = snapshot.best_bid_price()?;
        let best_ask = snapshot.best_ask_price()?;

        // Calculate base half-spread in ticks (priority: volatility > bps > fixed)
        // Quote validation rejects nonfinite prices before submission.
        let tick_size = self.tick_size();
        let base_half_spread_tick = if self.config.vol_to_half_spread > 0.0 && self.volatility > 0.0 {
            // Mode 1: Volatility-based (half_spread_price = volatility * vol_to_half_spread)
            (self.volatility * self.config.vol_to_half_spread) / tick_size
        } else if self.config.half_spread_bps > 0.0 {
            // Mode 2: BPS-based
            mid_price * (self.config.half_spread_bps / 10000.0) / tick_size
        } else if self.config.half_spread > 0.0 {
            // Mode 3: Fixed price
            self.config.half_spread / tick_size
        } else {
            // Fallback: use minimum spread
            1.0
        };

        // Calculate fair price (mid + alpha adjustment)
        let fair_price = mid_price + self.config.c1(tick_size) * self.alpha;

        // Get max_position_dollar from SharedEquity (lock-free read, ~1ns)
        // If not initialized, use f64::MAX (no normalization effect)
        let max_position_dollar = self
            .shared_equity
            .as_ref()
            .map(|eq| eq.max_position_dollar())
            .filter(|&v| v > 0.0)
            .unwrap_or(f64::MAX);

        // Calculate position skew (normalized to [-1, 1])
        let normalized_position = (self.position * mid_price) / max_position_dollar;
        let clamped_position = normalized_position.clamp(-1.0, 1.0);

        // Number of levels to generate (1 or 2)
        let num_levels = self.config.order_levels.min(MAX_ORDER_LEVELS);

        // Spread multipliers: level 0 = 1.0x, level 1 = spread_level_multiplier
        let multipliers = [1.0, self.config.spread_level_multiplier];

        // Get order_qty_dollar from SharedEquity (lock-free read, ~1ns)
        // Skip quote if no sizing available (equity not yet fetched)
        let mut order_qty_dollar = self
            .shared_equity
            .as_ref()
            .filter(|eq| eq.is_initialized())
            .map(|eq| eq.order_qty_dollar())
            .unwrap_or(0.0);

        if let Some(max_order_qty_dollar) = self.config.max_order_qty_dollar {
            order_qty_dollar = order_qty_dollar.min(max_order_qty_dollar);
        }

        if order_qty_dollar <= 0.0 {
            return None;
        }

        // Calculate prices for each level
        let mut bid_prices = [0.0; MAX_ORDER_LEVELS];
        let mut ask_prices = [0.0; MAX_ORDER_LEVELS];
        let mut bid_floored = [false; MAX_ORDER_LEVELS];
        let mut ask_floored = [false; MAX_ORDER_LEVELS];

        for level in 0..num_levels {
            let half_spread_tick = base_half_spread_tick * multipliers[level];

            // Adjust half-spread based on position
            // When long (positive position), increase bid depth (push bid down)
            // When short (negative position), increase ask depth (push ask up)
            let bid_depth_tick = (half_spread_tick * (1.0 + self.config.skew * clamped_position)).max(0.0);
            let ask_depth_tick = (half_spread_tick * (1.0 - self.config.skew * clamped_position)).max(0.0);

            // Calculate raw quote prices
            let raw_bid = fair_price - bid_depth_tick * tick_size;
            let raw_ask = fair_price + ask_depth_tick * tick_size;

            // Clamp to BBO (never cross the spread)
            let clamped_bid = raw_bid.min(best_bid);
            let clamped_ask = raw_ask.max(best_ask);

            // Apply floor AFTER BBO clamping
            // Floor ensures minimum distance from mid_price
            let (floored_bid, bid_floor_applied) = if self.config.min_half_spread_bps > 0.0 {
                // For outer levels, scale the minimum floor by the multiplier
                let level_min_bps = self.config.min_half_spread_bps * multipliers[level];
                let min_bid = mid_price * (1.0 - level_min_bps / 10000.0);
                if clamped_bid > min_bid {
                    (min_bid, true)
                } else {
                    (clamped_bid, false)
                }
            } else {
                (clamped_bid, false)
            };

            let (floored_ask, ask_floor_applied) = if self.config.min_half_spread_bps > 0.0 {
                let level_min_bps = self.config.min_half_spread_bps * multipliers[level];
                let min_ask = mid_price * (1.0 + level_min_bps / 10000.0);
                if clamped_ask < min_ask {
                    (min_ask, true)
                } else {
                    (clamped_ask, false)
                }
            } else {
                (clamped_ask, false)
            };

            // Snap to tick grid
            bid_prices[level] = (floored_bid / tick_size).floor() * tick_size;
            ask_prices[level] = (floored_ask / tick_size).ceil() * tick_size;
            bid_floored[level] = bid_floor_applied;
            ask_floored[level] = ask_floor_applied;
        }

        // Calculate quantity per level (round to lot_size, ensure minimum)
        // Note: order_qty_dollar is already per-order (formula includes /order_levels)
        let order_qty_per_level = order_qty_dollar / mid_price;
        let lot_size = self.lot_size();
        let quantity = (order_qty_per_level / lot_size).floor() * lot_size;

        // Log "valid for trading" milestone once (uses info! for visibility)
        let valid_for_trading = self.is_valid_for_trading();
        if valid_for_trading && !self.logged_valid_for_trading {
            self.logged_valid_for_trading = true;
            info!(
                "[{}] Strategy now valid for trading (history={:.0}s, samples={}, levels={})",
                snapshot.symbol,
                self.history_duration_secs(),
                self.total_samples,
                num_levels
            );
        }

        // Log quote generation with key metrics
        debug!(
            "[{}] Quote: vol={:.4} alpha={:.3} levels={} L0_spread={:.2}bps",
            snapshot.symbol,
            self.volatility,
            self.alpha,
            num_levels,
            (ask_prices[0] - bid_prices[0]) / mid_price * 10000.0
        );

        Some(Quote {
            symbol: snapshot.symbol,
            bid_prices,
            ask_prices,
            num_levels,
            quantity,
            mid_price,
            spread: ask_prices[0] - bid_prices[0],
            volatility: self.volatility,
            alpha: self.alpha,
            position: self.position,
            half_spread_tick: base_half_spread_tick,
            valid_for_trading,
            history_secs: self.history_duration_secs(),
            bid_floored,
            ask_floored,
        })
    }

    /// Reset the strategy state.
    pub fn reset_state(&mut self) {
        self.mid_price_chg_stats.clear();
        self.imbalance_stats.clear();
        self.prev_mid_price = None;
        self.held_observation = None;
        self.next_sample_ns = 0;
        self.last_observation_ns = 0;
        self.step_count = 0;
        self.last_update_step = 0;
        self.volatility = 0.0;
        self.alpha = 0.0;
        self.warmed_up = false;
        self.first_timestamp_ns = None;
        self.latest_timestamp_ns = 0;
        self.total_samples = 0;
        self.logged_valid_for_trading = false;
        self.logged_binance_alpha_active = false;
    }

    /// Check if using Binance alpha.
    #[inline]
    pub fn is_using_binance_alpha(&self) -> bool {
        self.use_binance_alpha
            && self.shared_binance_alpha.as_ref().is_some_and(|b| {
                b.is_warmed_up() && !b.is_stale(self.binance_stale_ms)
            })
    }

    /// Get the configured alpha source.
    #[inline]
    pub fn alpha_source(&self) -> &str {
        &self.config.alpha_source
    }
}

// ============================================================================
// QuoteStrategy trait implementation
// ============================================================================

impl QuoteStrategy for ObiStrategy {
    #[inline]
    fn update(&mut self, snapshot: &OrderbookSnapshot) -> Option<Quote> {
        // Delegate to the existing update method
        ObiStrategy::update(self, snapshot)
    }

    #[inline]
    fn set_position(&mut self, position: f64) {
        self.position = position;
    }

    #[inline]
    fn position(&self) -> f64 {
        self.position
    }

    #[inline]
    fn is_valid_for_trading(&self) -> bool {
        ObiStrategy::is_valid_for_trading(self)
    }

    #[inline]
    fn is_warmed_up(&self) -> bool {
        self.warmed_up
    }

    #[inline]
    fn volatility(&self) -> f64 {
        self.volatility
    }

    #[inline]
    fn alpha(&self) -> f64 {
        self.alpha
    }

    #[inline]
    fn history_duration_secs(&self) -> f64 {
        ObiStrategy::history_duration_secs(self)
    }

    #[inline]
    fn required_history_secs(&self) -> f64 {
        ObiStrategy::required_history_secs(self)
    }

    fn reset(&mut self) {
        self.reset_state();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Symbol;

    fn default_config() -> StrategyConfig {
        StrategyConfig {
            tick_size: 0.01,
            step_ns: 100_000_000,
            window_steps: 10,  // Small window for testing
            update_interval_steps: 1,
            vol_to_half_spread: 0.8,
            half_spread: 0.0,
            half_spread_bps: 0.0,
            skew: 1.0,
            c1: 0.0, // Use c1_ticks fallback
            c1_ticks: 160.0,
            looking_depth: 0.025,
            min_order_qty_dollar: 10.0,
            max_order_qty_dollar: None,
            lot_size: 0.001,
            min_half_spread_bps: 2.0,
            order_levels: 1,
            spread_level_multiplier: 1.5,
            alpha_source: "standx".to_string(), // Use StandX for tests
            binance_stale_ms: 5000,
            leverage: 1.0,
        }
    }

    /// Create a SharedEquity with initialized equity for tests
    fn test_shared_equity() -> Arc<SharedEquity> {
        let equity = Arc::new(SharedEquity::new(1, 10.0, 1.0));
        equity.set_equity(500.0); // $500 -> $90/order
        equity
    }

    fn create_snapshot(best_bid: f64, best_ask: f64) -> OrderbookSnapshot {
        let mut snapshot = OrderbookSnapshot::new(Symbol::new("TEST-USD"));

        // Add bid and ask levels
        let bids = vec![
            (best_bid, 1.0),
            (best_bid - 0.01, 2.0),
            (best_bid - 0.02, 3.0),
        ];
        let asks = vec![
            (best_ask, 1.0),
            (best_ask + 0.01, 2.0),
            (best_ask + 0.02, 3.0),
        ];

        snapshot.set_bids(&bids, 20);
        snapshot.set_asks(&asks, 20);
        snapshot
    }

    #[test]
    fn test_strategy_warmup() {
        let config = default_config();
        let shared_equity = test_shared_equity();
        let mut strategy = ObiStrategy::new(config, None, Some(shared_equity), None, 0, 5);

        // Feed snapshots until warmed up (need MIN_SAMPLES_FOR_QUOTE = 100)
        // First call has no prev_mid_tick so doesn't add to mid_price_chg_stats
        // So we need 101+ calls to get 100+ samples
        // Add some randomness to prices to get non-zero volatility
        let mut quote_received = false;
        for i in 0..200 {
            // Vary prices to create non-zero volatility
            let noise = if i % 3 == 0 { 0.05 } else if i % 3 == 1 { -0.03 } else { 0.02 };
            let mid = 100.0 + noise;
            let mut snapshot = create_snapshot(mid - 0.01, mid + 0.01);
            snapshot.received_at_ns = i * 100_000_000;
            let result = strategy.update(&snapshot);
            if result.is_some() {
                quote_received = true;
            }
        }

        assert!(strategy.is_warmed_up(), "Strategy should be warmed up after 200 iterations");
        assert!(quote_received, "Should have received at least one quote");
    }

    #[test]
    fn test_position_skew() {
        let config = default_config();
        let shared_equity = test_shared_equity();
        let mut strategy = ObiStrategy::new(config, None, Some(shared_equity), None, 0, 5);

        // Warm up (need MIN_SAMPLES_FOR_QUOTE = 100)
        // Add varying prices to create non-zero volatility
        for i in 0..200 {
            let noise = if i % 3 == 0 { 0.05 } else if i % 3 == 1 { -0.03 } else { 0.02 };
            let mid = 100.0 + noise;
            let mut snapshot = create_snapshot(mid - 0.01, mid + 0.01);
            snapshot.received_at_ns = i * 100_000_000;
            strategy.update(&snapshot);
        }

        assert!(strategy.is_warmed_up(), "Strategy should be warmed up");

        // Get quote with zero position
        let snapshot = create_snapshot(99.99, 100.01);
        strategy.set_position(0.0);
        let quote_neutral = strategy.calculate_quote(&snapshot, 100.0);

        // Get quote with long position
        strategy.set_position(5.0);  // 5 BTC long
        let quote_long = strategy.calculate_quote(&snapshot, 100.0);

        // Long inventory pushes the bid away from mid and the ask toward it.
        let (neutral, long) = (quote_neutral.unwrap(), quote_long.unwrap());
        assert!(long.bid_prices[0] < neutral.bid_prices[0], "{long:?} vs {neutral:?}");
        assert!(long.ask_prices[0] < neutral.ask_prices[0], "{long:?} vs {neutral:?}");
        assert!(long.ask_prices[0] > long.bid_prices[0]);
    }

    #[test]
    fn test_imbalance_calculation() {
        let config = default_config();
        let strategy = ObiStrategy::new(config, None, None, None, 0, 5);

        // Create snapshot with more bids than asks
        let mut snapshot = OrderbookSnapshot::new(Symbol::new("TEST-USD"));
        let bids = vec![
            (99.99, 10.0),  // Large bid
            (99.98, 5.0),
            (90.0, 1000.0), // Outside looking_depth (2.5%): ignored
        ];
        let asks = vec![
            (100.01, 1.0),  // Small ask
            (100.02, 1.0),
        ];
        snapshot.set_bids(&bids, 20);
        snapshot.set_asks(&asks, 20);

        let mid = snapshot.mid_price().unwrap();
        let imbalance = strategy.calculate_imbalance(&snapshot, mid);

        // 15 within-depth bids minus 2 asks; the deep 1000 is excluded.
        assert_eq!(imbalance, 13.0);
    }

    #[test]
    fn test_no_quote_without_equity() {
        let config = default_config();
        // Create strategy WITHOUT shared_equity
        let mut strategy = ObiStrategy::new(config, None, None, None, 0, 5);

        // Warm up
        for i in 0..200 {
            let noise = if i % 3 == 0 { 0.05 } else if i % 3 == 1 { -0.03 } else { 0.02 };
            let mid = 100.0 + noise;
            let mut snapshot = create_snapshot(mid - 0.01, mid + 0.01);
            snapshot.received_at_ns = i * 100_000_000;
            let _ = strategy.update(&snapshot);
        }

        // Should be warmed up but no quotes (no equity)
        assert!(strategy.is_warmed_up());

        let snapshot = create_snapshot(99.99, 100.01);
        let quote = strategy.update(&snapshot);
        assert!(quote.is_none(), "Should not generate quote without equity");
    }

    #[test]
    fn trading_requires_ten_continuous_minutes_and_reset_restarts_clock() {
        let config = default_config();
        let shared_equity = test_shared_equity();
        let mut strategy = ObiStrategy::new(config, None, Some(shared_equity), None, 10, 5);
        let base = 1_800_000_000_000_000_000_i64;

        for index in 0..=598_i64 {
            let mut snapshot = create_snapshot(99.99, 100.01);
            snapshot.received_at_ns = base + index * 1_000_000_000;
            let _ = strategy.update(&snapshot);
        }
        assert!(strategy.is_warmed_up());
        assert!(!strategy.is_valid_for_trading());

        let mut before_ten_minutes = create_snapshot(99.99, 100.01);
        before_ten_minutes.received_at_ns = base + 599_000_000_000;
        let quote = strategy.update(&before_ten_minutes).unwrap();
        assert!(!quote.valid_for_trading);

        let mut at_ten_minutes = create_snapshot(99.99, 100.01);
        at_ten_minutes.received_at_ns = base + 600_000_000_000;
        let quote = strategy.update(&at_ten_minutes).unwrap();
        assert!(quote.valid_for_trading);

        strategy.reset_state();
        assert!(!strategy.is_warmed_up());
        assert!(!strategy.is_valid_for_trading());
        assert_eq!(strategy.history_duration_ns(), 0);
    }

    #[test]
    fn grid_is_invariant_to_redundant_message_frequency_and_never_looks_ahead() {
        let mut config = default_config();
        config.window_steps = 100;
        let mut sparse = ObiStrategy::new(config.clone(), None, None, None, 0, 5);
        let mut dense = ObiStrategy::new(config, None, None, None, 0, 5);
        for i in 0..=200 {
            let mid = 100.0 + (i % 3) as f64;
            let mut book = create_snapshot(mid - 0.01, mid + 0.01);
            book.received_at_ns = 1 + i * 100_000_000;
            sparse.update(&book);
            dense.update(&book);
            if i < 200 {
                for extra in [20_000_000, 70_000_000] {
                    book.received_at_ns = 1 + i * 100_000_000 + extra;
                    dense.update(&book);
                }
            }
        }
        assert_eq!(sparse.total_samples, dense.total_samples);
        assert!((sparse.volatility - dense.volatility).abs() < 1e-12);
        // The grid increments are 1, 1, -2, with variance near 2 price^2.
        assert!(sparse.volatility > 4.3 && sparse.volatility < 4.6);
        let mut delayed = create_snapshot(199.99, 200.01);
        delayed.received_at_ns = 1 + 20_250_000_000;
        let samples_before = sparse.total_samples;
        sparse.update(&delayed);
        assert_eq!(sparse.total_samples, samples_before + 2);
        // At 20.1 and 20.2 seconds only the old observation was available.
        assert_eq!(sparse.prev_mid_price, Some(102.0));
        delayed.received_at_ns = 1 + 20_300_000_000;
        sparse.update(&delayed);
        assert_eq!(sparse.prev_mid_price, Some(200.0));
    }

    #[test]
    fn flat_prices_have_zero_volatility_and_stale_gaps_reset_warmup() {
        let mut strategy = ObiStrategy::new(default_config(), None, None, None, 0, 5);
        let mut book = create_snapshot(99.99, 100.01);
        for i in 0..200 { book.received_at_ns = 1 + i * 100_000_000; strategy.update(&book); }
        assert!(strategy.is_warmed_up());
        assert_eq!(strategy.volatility(), 0.0);
        book.received_at_ns += 5_000_000_000;
        strategy.update(&book);
        assert!(!strategy.is_warmed_up());
        assert_eq!(strategy.total_samples, 0);
        assert_eq!(strategy.history_duration_ns(), 0);
    }

    #[test]
    fn legacy_alpha_coefficient_uses_exchange_tick_in_actual_quotes() {
        let config = StrategyConfig { c1_ticks: 2.0, half_spread: 10.0, vol_to_half_spread: 0.0, ..default_config() };
        // API tick differs from fallback config.tick_size by a factor of 100.
        let info = Arc::new(SharedSymbolInfo::new("TEST-USD", 1.0, 0.001));
        let mut strategy = ObiStrategy::new(config.clone(), Some(Arc::clone(&info)), Some(test_shared_equity()), None, 0, 5);
        strategy.alpha = 1.0;
        let book = create_snapshot(99.0, 101.0);
        let quote = strategy.calculate_quote(&book, 100.0).unwrap();
        assert_eq!(quote.bid_prices[0], 92.0);
        assert_eq!(quote.ask_prices[0], 112.0);
        let mut explicit = ObiStrategy::new(StrategyConfig { c1: 3.0, ..config }, Some(info), Some(test_shared_equity()), None, 0, 5);
        explicit.alpha = 1.0;
        assert_eq!(explicit.calculate_quote(&book, 100.0).unwrap().bid_prices[0], 93.0);
    }
}
