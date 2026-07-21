//! Thread-safe current orderbook storage.
//!
//! Snapshot storage is deliberately kept off the quote-to-order path. A compact
//! `parking_lot` lock makes the ownership rules explicit and supports the market
//! stream and the infrequent REST sanity correction without unsafe aliasing.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::types::{OrderbookSnapshot, Symbol};

/// Thread-safe storage for the most recently published orderbook snapshot.
pub struct CurrentOrderbook {
    snapshot: RwLock<Option<OrderbookSnapshot>>,

    /// Sequence number for updates
    update_sequence: AtomicU64,

    /// Symbol this orderbook is for
    symbol: Symbol,
}

impl CurrentOrderbook {
    /// Create a new triple buffer for the given symbol.
    pub fn new(symbol: Symbol) -> Self {
        Self {
            snapshot: RwLock::new(None),
            update_sequence: AtomicU64::new(0),
            symbol,
        }
    }

    /// Get the symbol this orderbook is for.
    #[inline]
    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    /// Update the current orderbook state.
    ///
    /// The update function receives exclusive access to the published snapshot.
    pub fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut OrderbookSnapshot),
    {
        let mut snapshot = self.snapshot.write();
        let buffer = snapshot.get_or_insert_with(|| OrderbookSnapshot::new(self.symbol));
        f(buffer);
        self.update_sequence.fetch_add(1, Ordering::Release);
    }

    /// Update with a complete snapshot replacement.
    pub fn update_snapshot(&self, snapshot: OrderbookSnapshot) {
        *self.snapshot.write() = Some(snapshot);
        self.update_sequence.fetch_add(1, Ordering::Release);
    }

    /// Read the current orderbook state.
    ///
    /// Returns a fixed-size clone of the current state.
    /// Returns `None` if no data has been written yet.
    pub fn read(&self) -> Option<OrderbookSnapshot> {
        self.snapshot.read().clone()
    }

    /// Read the current state. Kept as a compatibility alias for `read`.
    pub fn peek(&self) -> Option<OrderbookSnapshot> {
        self.read()
    }

    /// Get the best bid price without copying the entire snapshot.
    #[inline]
    pub fn best_bid(&self) -> Option<f64> {
        self.snapshot
            .read()
            .as_ref()
            .and_then(OrderbookSnapshot::best_bid_price)
    }

    /// Get the best ask price without copying the entire snapshot.
    #[inline]
    pub fn best_ask(&self) -> Option<f64> {
        self.snapshot
            .read()
            .as_ref()
            .and_then(OrderbookSnapshot::best_ask_price)
    }

    /// Get the current spread without copying the entire snapshot.
    #[inline]
    pub fn spread(&self) -> Option<f64> {
        self.snapshot
            .read()
            .as_ref()
            .and_then(OrderbookSnapshot::spread)
    }

    /// Get the current mid price without copying the entire snapshot.
    #[inline]
    pub fn mid_price(&self) -> Option<f64> {
        self.snapshot
            .read()
            .as_ref()
            .and_then(OrderbookSnapshot::mid_price)
    }

    /// Get the update sequence number.
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.update_sequence.load(Ordering::Acquire)
    }

    /// Check if data has been written.
    #[inline]
    pub fn has_data(&self) -> bool {
        self.snapshot.read().is_some()
    }
}

/// Manager for multiple symbols' orderbooks.
///
/// Provides efficient lookup and iteration over orderbooks
/// for multiple trading pairs.
pub struct OrderbookManager {
    /// Orderbooks indexed by symbol
    orderbooks: Vec<CurrentOrderbook>,
    /// Symbol lookup map (linear search is fine for small N)
    symbols: Vec<Symbol>,
}

impl OrderbookManager {
    /// Create a new orderbook manager for the given symbols.
    pub fn new(symbols: &[String]) -> Self {
        let symbols: Vec<Symbol> = symbols.iter().map(|s| Symbol::new(s)).collect();
        let orderbooks = symbols.iter().map(|s| CurrentOrderbook::new(*s)).collect();

        Self {
            orderbooks,
            symbols,
        }
    }

    /// Get the orderbook for a symbol.
    pub fn get(&self, symbol: &str) -> Option<&CurrentOrderbook> {
        let sym = Symbol::new(symbol);
        self.symbols
            .iter()
            .position(|s| s == &sym)
            .map(|idx| &self.orderbooks[idx])
    }

    /// Get all orderbooks.
    pub fn all(&self) -> &[CurrentOrderbook] {
        &self.orderbooks
    }

    /// Get the number of orderbooks.
    pub fn len(&self) -> usize {
        self.orderbooks.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.orderbooks.is_empty()
    }

    /// Iterate over all symbols.
    pub fn symbols(&self) -> impl Iterator<Item = &Symbol> {
        self.symbols.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PriceLevel;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_triple_buffer_basic() {
        let ob = CurrentOrderbook::new(Symbol::new("TEST-USD"));

        // Initially no data
        assert!(!ob.has_data());
        assert!(ob.read().is_none());

        // Write some data
        ob.update(|snapshot| {
            snapshot.bids[0] = PriceLevel::new(100.0, 1.0);
            snapshot.bid_count = 1;
            snapshot.asks[0] = PriceLevel::new(101.0, 1.0);
            snapshot.ask_count = 1;
        });

        // Now we should have data
        assert!(ob.has_data());
        let snapshot = ob.read().unwrap();
        assert_eq!(snapshot.best_bid_price(), Some(100.0));
        assert_eq!(snapshot.best_ask_price(), Some(101.0));
    }

    #[test]
    fn test_triple_buffer_updates() {
        let ob = CurrentOrderbook::new(Symbol::new("TEST-USD"));

        // Multiple updates
        for i in 0..10 {
            ob.update(|snapshot| {
                snapshot.bids[0] = PriceLevel::new(100.0 + i as f64, 1.0);
                snapshot.bid_count = 1;
                snapshot.sequence = i;
            });
        }

        let snapshot = ob.read().unwrap();
        assert_eq!(snapshot.sequence, 9);
        assert_eq!(snapshot.best_bid_price(), Some(109.0));
    }

    #[test]
    fn test_orderbook_manager() {
        let manager = OrderbookManager::new(&["TEST-USD".to_string(), "ETH-USD".to_string()]);

        assert_eq!(manager.len(), 2);

        let btc = manager.get("TEST-USD").unwrap();
        btc.update(|s| {
            s.bids[0] = PriceLevel::new(50000.0, 1.0);
            s.bid_count = 1;
        });

        let eth = manager.get("ETH-USD").unwrap();
        eth.update(|s| {
            s.bids[0] = PriceLevel::new(3000.0, 1.0);
            s.bid_count = 1;
        });

        assert_eq!(manager.get("TEST-USD").unwrap().best_bid(), Some(50000.0));
        assert_eq!(manager.get("ETH-USD").unwrap().best_bid(), Some(3000.0));
        assert!(manager.get("SOL-USD").is_none());
    }

    #[test]
    fn concurrent_writers_and_readers_publish_coherent_snapshots() {
        let orderbook = Arc::new(CurrentOrderbook::new(Symbol::new("TEST-USD")));
        let writers_done = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();

        for writer in 0..2_u64 {
            let orderbook = Arc::clone(&orderbook);
            let writers_done = Arc::clone(&writers_done);
            threads.push(std::thread::spawn(move || {
                for sequence in 1..=10_000_u64 {
                    let value = writer * 10_000 + sequence;
                    orderbook.update(|snapshot| {
                        snapshot.sequence = value;
                        snapshot.bids[0] = PriceLevel::new(value as f64, 1.0);
                        snapshot.bid_count = 1;
                    });
                }
                writers_done.fetch_add(1, Ordering::Release);
            }));
        }

        for _ in 0..4 {
            let orderbook = Arc::clone(&orderbook);
            let writers_done = Arc::clone(&writers_done);
            threads.push(std::thread::spawn(move || {
                while writers_done.load(Ordering::Acquire) < 2 {
                    if let Some(snapshot) = orderbook.read() {
                        assert_eq!(snapshot.best_bid_price(), Some(snapshot.sequence as f64));
                    }
                }
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(orderbook.sequence(), 20_000);
    }
}
