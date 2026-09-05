//! Current orderbook storage for diagnostic readers.

pub mod sanity_check;
mod snapshot;

pub use sanity_check::{OrderbookSanityChecker, SanityCheckerConfig, SanityCheckerHandle, SanityCheckerStats};
pub use snapshot::{CurrentOrderbook, OrderbookManager};

use crate::types::OrderbookSnapshot;

/// Combined orderbook state for a single symbol.
///
/// Provides current state access for diagnostics.
pub struct SymbolOrderbook {
    /// Current orderbook state
    pub current: CurrentOrderbook,
}

impl SymbolOrderbook {
    /// Create a new symbol orderbook.
    ///
    /// # Arguments
    ///
    /// * `symbol` - Trading symbol (e.g., "TEST-USD")
    pub fn new(symbol: &str) -> Self {
        let sym = crate::types::Symbol::new(symbol);
        Self {
            current: CurrentOrderbook::new(sym),
        }
    }

    /// Update the orderbook with a new snapshot.
    ///
    /// Publishes the current state.
    pub fn update(&self, snapshot: OrderbookSnapshot) {
        self.update_current(snapshot);
    }

    /// Publish the current snapshot for cold-path readers.
    pub fn update_current(&self, snapshot: OrderbookSnapshot) {
        self.current.update_snapshot(snapshot);
    }

    /// Get the latest orderbook snapshot.
    pub fn latest(&self) -> Option<OrderbookSnapshot> {
        self.current.read()
    }

    /// Get a snapshot without swapping indices.
    ///
    /// This is safe for concurrent readers (e.g., sanity checker)
    /// as it reads from the stable swap buffer without modifying state.
    pub fn peek(&self) -> Option<OrderbookSnapshot> {
        self.current.peek()
    }

    /// Get the best bid price.
    #[inline]
    pub fn best_bid(&self) -> Option<f64> {
        self.current.best_bid()
    }

    /// Get the best ask price.
    #[inline]
    pub fn best_ask(&self) -> Option<f64> {
        self.current.best_ask()
    }

    /// Get the current spread.
    #[inline]
    pub fn spread(&self) -> Option<f64> {
        self.current.spread()
    }

    /// Get the current mid price.
    #[inline]
    pub fn mid_price(&self) -> Option<f64> {
        self.current.mid_price()
    }

}

/// Manager for multiple symbol orderbooks .
pub struct OrderbookStore {
    /// Orderbooks by symbol
    orderbooks: Vec<SymbolOrderbook>,
    /// Symbol names for lookup
    symbols: Vec<String>,
}

impl OrderbookStore {
    /// Create a new orderbook store.
    ///
    /// # Arguments
    ///
    /// * `symbols` - List of trading symbols
    pub fn new(symbols: &[String]) -> Self {
        let orderbooks = symbols
            .iter()
            .map(|s| SymbolOrderbook::new(s))
            .collect();

        Self {
            orderbooks,
            symbols: symbols.to_vec(),
        }
    }

    /// Get the orderbook for a symbol.
    pub fn get(&self, symbol: &str) -> Option<&SymbolOrderbook> {
        self.symbols
            .iter()
            .position(|s| s == symbol)
            .map(|idx| &self.orderbooks[idx])
    }

    /// Get all orderbooks.
    pub fn all(&self) -> &[SymbolOrderbook] {
        &self.orderbooks
    }

    /// Get the number of symbols.
    pub fn len(&self) -> usize {
        self.orderbooks.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.orderbooks.is_empty()
    }

    /// Get all symbol names.
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// Get statistics for all orderbooks.
    pub fn stats(&self) -> Vec<OrderbookStats> {
        self.orderbooks
            .iter()
            .zip(self.symbols.iter())
            .map(|(ob, symbol)| OrderbookStats {
                symbol: symbol.clone(),
                has_data: ob.current.has_data(),
                update_count: ob.current.sequence(),
                best_bid: ob.best_bid(),
                best_ask: ob.best_ask(),
                spread: ob.spread(),
            })
            .collect()
    }
}

/// Statistics for a single orderbook.
#[derive(Debug, Clone)]
pub struct OrderbookStats {
    pub symbol: String,
    pub has_data: bool,
    pub update_count: u64,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_orderbook() {
        let ob = SymbolOrderbook::new("TEST-USD");

        let mut snapshot = OrderbookSnapshot::new(crate::types::Symbol::new("TEST-USD"));
        snapshot.set_bids(&[(100.0, 1.0)], 20);
        snapshot.set_asks(&[(101.0, 1.0)], 20);

        ob.update(snapshot);

        assert_eq!(ob.best_bid(), Some(100.0));
        assert_eq!(ob.best_ask(), Some(101.0));
        assert_eq!(ob.spread(), Some(1.0));
    }

    #[test]
    fn test_orderbook_store() {
        let store = OrderbookStore::new(
            &["TEST-USD".to_string(), "ETH-USD".to_string()],
        );

        assert_eq!(store.len(), 2);
        assert!(store.get("TEST-USD").is_some());
        assert!(store.get("ETH-USD").is_some());
        assert!(store.get("SOL-USD").is_none());
    }
}
