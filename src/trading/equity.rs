//! Lock-free equity storage for hot path access.
//!
//! This module provides `SharedEquity` which stores equity and pre-calculates
//! order_qty_dollar on write to avoid division in the hot path.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Lock-free equity storage for hot path access.
///
/// Pre-calculates order_qty_dollar on write to avoid division in hot path.
/// Formula: `order_qty_dollar = equity / 5 * 0.9 / order_levels`
///
/// # Example
///
/// With $500 equity and 2 order levels:
/// - $500 / 5 = $100 (use 20% of capital)
/// - $100 * 0.9 = $90 (10% safety margin)
/// - $90 / 2 = $45 per order (split across levels)
pub struct SharedEquity {
    /// Raw equity in USD, stored as f64 bits
    equity_bits: AtomicU64,
    /// Pre-calculated order_qty_dollar per order, stored as f64 bits
    order_qty_dollar_bits: AtomicU64,
    /// Number of order levels (for formula)
    order_levels: AtomicU8,
    /// Minimum order size in USD
    min_order_qty_dollar: f64,
    /// Last update timestamp (Unix millis)
    last_update_ms: AtomicU64,
}

impl SharedEquity {
    /// Create a new SharedEquity with the given order levels and minimum order size.
    ///
    /// # Arguments
    ///
    /// * `order_levels` - Number of order levels (1 or 2)
    /// * `min_order_qty_dollar` - Minimum order size in USD (prevents dust orders)
    pub fn new(order_levels: usize, min_order_qty_dollar: f64) -> Self {
        Self {
            equity_bits: AtomicU64::new(0.0f64.to_bits()),
            order_qty_dollar_bits: AtomicU64::new(0.0f64.to_bits()),
            order_levels: AtomicU8::new(order_levels as u8),
            min_order_qty_dollar,
            last_update_ms: AtomicU64::new(0),
        }
    }

    /// Lock-free read of pre-calculated order_qty_dollar.
    ///
    /// Returns 0.0 if equity not yet fetched.
    /// This is designed for the hot path (~1ns latency).
    #[inline]
    pub fn order_qty_dollar(&self) -> f64 {
        f64::from_bits(self.order_qty_dollar_bits.load(Ordering::Acquire))
    }

    /// Lock-free read of raw equity.
    #[inline]
    pub fn equity(&self) -> f64 {
        f64::from_bits(self.equity_bits.load(Ordering::Acquire))
    }

    /// Check if equity has been initialized (> 0).
    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.equity() > 0.0
    }

    /// Update equity and recalculate order_qty_dollar.
    ///
    /// Called from WalletTracker background task (cold path, 60s interval).
    /// The formula division happens here, not in the hot path.
    ///
    /// Formula: `equity / 5 * 0.9 / order_levels`
    pub fn set_equity(&self, equity: f64) {
        self.equity_bits.store(equity.to_bits(), Ordering::Release);

        // Calculate: equity / 5 * 0.9 / order_levels
        let levels = self.order_levels.load(Ordering::Acquire) as f64;
        let raw_qty = equity / 5.0 * 0.9 / levels;
        let qty = raw_qty.max(self.min_order_qty_dollar);

        self.order_qty_dollar_bits
            .store(qty.to_bits(), Ordering::Release);
        self.last_update_ms.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            Ordering::Release,
        );
    }

    /// Get milliseconds since last update.
    pub fn age_ms(&self) -> u64 {
        let last = self.last_update_ms.load(Ordering::Acquire);
        if last == 0 {
            return u64::MAX;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        now.saturating_sub(last)
    }

    /// Get the minimum order quantity in USD.
    #[inline]
    pub fn min_order_qty_dollar(&self) -> f64 {
        self.min_order_qty_dollar
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_equity_new() {
        let equity = SharedEquity::new(2, 10.0);
        assert_eq!(equity.equity(), 0.0);
        assert_eq!(equity.order_qty_dollar(), 0.0);
        assert!(!equity.is_initialized());
    }

    #[test]
    fn test_shared_equity_formula() {
        // Test with $500 equity and 2 order levels
        // Expected: $500 / 5 * 0.9 / 2 = $45
        let equity = SharedEquity::new(2, 10.0);
        equity.set_equity(500.0);

        assert!(equity.is_initialized());
        assert_eq!(equity.equity(), 500.0);
        assert_eq!(equity.order_qty_dollar(), 45.0);
    }

    #[test]
    fn test_shared_equity_formula_single_level() {
        // Test with $500 equity and 1 order level
        // Expected: $500 / 5 * 0.9 / 1 = $90
        let equity = SharedEquity::new(1, 10.0);
        equity.set_equity(500.0);

        assert_eq!(equity.order_qty_dollar(), 90.0);
    }

    #[test]
    fn test_shared_equity_min_floor() {
        // Test with very low equity (below min_order_qty_dollar floor)
        // Expected: min_order_qty_dollar = 10.0
        let equity = SharedEquity::new(2, 10.0);
        equity.set_equity(10.0); // $10 / 5 * 0.9 / 2 = $0.9, below min

        assert_eq!(equity.order_qty_dollar(), 10.0);
    }

    #[test]
    fn test_shared_equity_zero() {
        // Test with zero equity
        let equity = SharedEquity::new(2, 10.0);
        equity.set_equity(0.0);

        assert!(!equity.is_initialized());
        assert_eq!(equity.order_qty_dollar(), 10.0); // Falls back to min
    }

    #[test]
    fn test_shared_equity_age() {
        let equity = SharedEquity::new(2, 10.0);

        // Before any update, age should be MAX
        assert_eq!(equity.age_ms(), u64::MAX);

        // After update, age should be small
        equity.set_equity(100.0);
        assert!(equity.age_ms() < 1000); // Less than 1 second
    }
}
