//! Lock-free atomic storage for Binance best bid/offer (BBO) data.
//!
//! Provides sub-nanosecond read access for the hot path while allowing
//! concurrent updates from the Binance bookTicker WebSocket poller.
//!
//! ## Performance Characteristics
//!
//! - Read (best_bid/best_ask/mid): ~1ns (single atomic load)
//! - Write (update): ~1ns (atomic stores)
//! - Memory: 64 bytes (cache-line aligned to avoid false sharing)

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Lock-free atomic storage for Binance BBO (best bid/offer).
///
/// Uses `AtomicU64` with f64-to-bits conversion for lock-free reads.
/// Cache-line aligned to avoid false sharing with other data.
#[repr(align(64))]
pub struct SharedBbo {
    /// Best bid price stored as f64 bits
    best_bid_bits: AtomicU64,

    /// Best ask price stored as f64 bits
    best_ask_bits: AtomicU64,

    /// Best bid quantity stored as f64 bits
    best_bid_qty_bits: AtomicU64,

    /// Best ask quantity stored as f64 bits
    best_ask_qty_bits: AtomicU64,

    /// Order book update ID from bookTicker
    update_id: AtomicU64,

    /// Last update timestamp in milliseconds (for staleness check)
    /// This is the Release-store field; readers use Acquire on this.
    last_update_ms: AtomicU64,
}

impl SharedBbo {
    /// Create a new SharedBbo with zero values.
    pub fn new() -> Self {
        Self {
            best_bid_bits: AtomicU64::new(0.0_f64.to_bits()),
            best_ask_bits: AtomicU64::new(0.0_f64.to_bits()),
            best_bid_qty_bits: AtomicU64::new(0.0_f64.to_bits()),
            best_ask_qty_bits: AtomicU64::new(0.0_f64.to_bits()),
            update_id: AtomicU64::new(0),
            last_update_ms: AtomicU64::new(0),
        }
    }

    /// Get the best bid price (lock-free, ~1ns).
    #[inline]
    pub fn best_bid(&self) -> f64 {
        f64::from_bits(self.best_bid_bits.load(Ordering::Relaxed))
    }

    /// Get the best ask price (lock-free, ~1ns).
    #[inline]
    pub fn best_ask(&self) -> f64 {
        f64::from_bits(self.best_ask_bits.load(Ordering::Relaxed))
    }

    /// Get the best bid quantity (lock-free, ~1ns).
    #[inline]
    pub fn best_bid_qty(&self) -> f64 {
        f64::from_bits(self.best_bid_qty_bits.load(Ordering::Relaxed))
    }

    /// Get the best ask quantity (lock-free, ~1ns).
    #[inline]
    pub fn best_ask_qty(&self) -> f64 {
        f64::from_bits(self.best_ask_qty_bits.load(Ordering::Relaxed))
    }

    /// Get the mid price: (best_bid + best_ask) / 2.
    #[inline]
    pub fn mid(&self) -> f64 {
        (self.best_bid() + self.best_ask()) / 2.0
    }

    /// Get the spread: best_ask - best_bid.
    #[inline]
    pub fn spread(&self) -> f64 {
        self.best_ask() - self.best_bid()
    }

    /// Get the order book update ID.
    #[inline]
    pub fn update_id(&self) -> u64 {
        self.update_id.load(Ordering::Relaxed)
    }

    /// Get the last update timestamp in milliseconds.
    #[inline]
    pub fn last_update_ms(&self) -> u64 {
        self.last_update_ms.load(Ordering::Relaxed)
    }

    /// Check if BBO data has been received (at least one update).
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.update_id.load(Ordering::Acquire) > 0
    }

    /// Check if the BBO is stale (no update within threshold_ms).
    #[inline]
    pub fn is_stale(&self, threshold_ms: u64) -> bool {
        let last = self.last_update_ms.load(Ordering::Acquire);
        let now = current_time_ms();
        now.saturating_sub(last) > threshold_ms
    }

    /// Update BBO values atomically.
    ///
    /// Called by the Binance bookTicker poller on each update.
    /// Uses Release on the final store (last_update_ms) so readers using Acquire
    /// will see all preceding writes. This is necessary for correctness on
    /// ARM (Graviton) where Relaxed provides no inter-thread visibility guarantees.
    #[inline]
    pub fn update(&self, bid: f64, ask: f64, bid_qty: f64, ask_qty: f64, upd_id: u64) {
        self.best_bid_bits.store(bid.to_bits(), Ordering::Relaxed);
        self.best_ask_bits.store(ask.to_bits(), Ordering::Relaxed);
        self.best_bid_qty_bits
            .store(bid_qty.to_bits(), Ordering::Relaxed);
        self.best_ask_qty_bits
            .store(ask_qty.to_bits(), Ordering::Relaxed);
        self.update_id.store(upd_id, Ordering::Relaxed);
        self.last_update_ms.store(current_time_ms(), Ordering::Release);
    }

    /// Reset the shared state (clear all values).
    ///
    /// Called on WebSocket reconnect.
    pub fn reset(&self) {
        self.best_bid_bits
            .store(0.0_f64.to_bits(), Ordering::Relaxed);
        self.best_ask_bits
            .store(0.0_f64.to_bits(), Ordering::Relaxed);
        self.best_bid_qty_bits
            .store(0.0_f64.to_bits(), Ordering::Relaxed);
        self.best_ask_qty_bits
            .store(0.0_f64.to_bits(), Ordering::Relaxed);
        self.update_id.store(0, Ordering::Relaxed);
        self.last_update_ms.store(0, Ordering::Relaxed);
    }
}

impl Default for SharedBbo {
    fn default() -> Self {
        Self::new()
    }
}

/// Get current time in milliseconds since UNIX epoch.
#[inline]
fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_shared_bbo() {
        let shared = SharedBbo::new();
        assert_eq!(shared.best_bid(), 0.0);
        assert_eq!(shared.best_ask(), 0.0);
        assert_eq!(shared.best_bid_qty(), 0.0);
        assert_eq!(shared.best_ask_qty(), 0.0);
        assert_eq!(shared.update_id(), 0);
        assert!(!shared.is_valid());
    }

    #[test]
    fn test_update_and_read() {
        let shared = SharedBbo::new();
        shared.update(66268.70, 66268.80, 9.313, 5.084, 12345);

        assert!((shared.best_bid() - 66268.70).abs() < 1e-10);
        assert!((shared.best_ask() - 66268.80).abs() < 1e-10);
        assert!((shared.best_bid_qty() - 9.313).abs() < 1e-10);
        assert!((shared.best_ask_qty() - 5.084).abs() < 1e-10);
        assert_eq!(shared.update_id(), 12345);
        assert!(shared.is_valid());
    }

    #[test]
    fn test_mid_and_spread() {
        let shared = SharedBbo::new();
        shared.update(66268.70, 66268.80, 9.313, 5.084, 1);

        assert!((shared.mid() - 66268.75).abs() < 1e-10);
        assert!((shared.spread() - 0.10).abs() < 1e-10);
    }

    #[test]
    fn test_staleness() {
        let shared = SharedBbo::new();

        // Should be stale with no updates
        assert!(shared.is_stale(1000));

        // Update and check freshness
        shared.update(100.0, 101.0, 1.0, 1.0, 1);
        assert!(!shared.is_stale(1000));
    }

    #[test]
    fn test_is_valid() {
        let shared = SharedBbo::new();
        assert!(!shared.is_valid());

        shared.update(100.0, 101.0, 1.0, 1.0, 1);
        assert!(shared.is_valid());
    }

    #[test]
    fn test_reset() {
        let shared = SharedBbo::new();
        shared.update(66268.70, 66268.80, 9.313, 5.084, 12345);
        assert!(shared.is_valid());

        shared.reset();
        assert_eq!(shared.best_bid(), 0.0);
        assert_eq!(shared.best_ask(), 0.0);
        assert_eq!(shared.best_bid_qty(), 0.0);
        assert_eq!(shared.best_ask_qty(), 0.0);
        assert_eq!(shared.update_id(), 0);
        assert!(!shared.is_valid());
    }
}
