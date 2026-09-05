//! Core data types for the StandX orderbook parser.
//!
//! These types are designed for high performance with minimal allocations:
//! - Fixed-size arrays for price levels
//! - Copy semantics where possible
//! - No heap allocations in hot paths

use std::fmt;

/// Maximum number of price levels supported.
/// This is a compile-time constant to enable fixed-size arrays.
pub const MAX_LEVELS: usize = 50;

/// A single price level in the orderbook.
///
/// Uses f64 for performance. For applications requiring exact decimal
/// precision, consider using rust_decimal in the parsing layer and
/// converting to f64 only for calculations.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
#[repr(C)]
pub struct PriceLevel {
    pub price: f64,
    pub quantity: f64,
}

impl PriceLevel {
    /// Create a new price level.
    #[inline]
    pub const fn new(price: f64, quantity: f64) -> Self {
        Self { price, quantity }
    }

    /// Check if this level is empty (zero price and quantity).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.price == 0.0 && self.quantity == 0.0
    }

    /// Calculate the notional value (price * quantity).
    #[inline]
    pub fn notional(&self) -> f64 {
        self.price * self.quantity
    }
}

/// Fixed-size symbol string to avoid heap allocations.
/// Supports symbols up to 15 characters (plus null terminator).
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Symbol {
    data: [u8; 16],
    len: u8,
}

/// Error returned when a symbol cannot be represented without truncation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolError;

impl fmt::Display for SymbolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "symbol must be at most 15 UTF-8 bytes")
    }
}

impl std::error::Error for SymbolError {}

impl Symbol {
    /// Create a new symbol from a string slice.
    ///
    /// This compatibility constructor truncates at a valid UTF-8 boundary. Runtime
    /// configuration should use [`Symbol::try_new`] so aliases cannot be introduced.
    pub fn new(s: &str) -> Self {
        let bytes = s.as_bytes();
        let mut len = bytes.len().min(15);
        while !s.is_char_boundary(len) {
            len -= 1;
        }
        let mut data = [0u8; 16];
        data[..len].copy_from_slice(&bytes[..len]);
        Self {
            data,
            len: len as u8,
        }
    }

    /// Create a symbol without truncation.
    pub fn try_new(s: &str) -> Result<Self, SymbolError> {
        if s.len() > 15 {
            return Err(SymbolError);
        }
        Ok(Self::new(s))
    }

    /// Get the symbol as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.data[..self.len as usize])
            .expect("Symbol constructors preserve UTF-8 boundaries")
    }

    /// Get the length of the symbol.
    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Check if the symbol is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Symbol(\"{}\")", self.as_str())
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl AsRef<str> for Symbol {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// A complete orderbook snapshot at a point in time.
///
/// This struct is designed for lock-free operations:
/// - Fixed-size arrays (no heap allocation)
/// - Copy-friendly layout
/// - Sequence number for consistency checks
#[derive(Clone)]
pub struct OrderbookSnapshot {
    /// Trading symbol (e.g., "TEST-USD")
    pub symbol: Symbol,

    /// Server timestamp in nanoseconds since Unix epoch
    pub timestamp_ns: i64,

    /// Adapter receipt clock in nanoseconds; StandX uses process-local monotonic time
    pub received_at_ns: i64,

    /// Exchange sequence number for ordering
    pub sequence: u64,

    /// Bid levels, sorted descending by price (best bid first)
    pub bids: [PriceLevel; MAX_LEVELS],

    /// Ask levels, sorted ascending by price (best ask first)
    pub asks: [PriceLevel; MAX_LEVELS],

    /// Number of valid bid levels
    pub bid_count: u8,

    /// Number of valid ask levels
    pub ask_count: u8,
}

impl Default for OrderbookSnapshot {
    fn default() -> Self {
        Self {
            symbol: Symbol::default(),
            timestamp_ns: 0,
            received_at_ns: 0,
            sequence: 0,
            bids: [PriceLevel::default(); MAX_LEVELS],
            asks: [PriceLevel::default(); MAX_LEVELS],
            bid_count: 0,
            ask_count: 0,
        }
    }
}

impl OrderbookSnapshot {
    /// Create a new empty orderbook snapshot.
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            ..Default::default()
        }
    }

    /// Get the best bid price and quantity, if available.
    #[inline]
    pub fn best_bid(&self) -> Option<PriceLevel> {
        if self.bid_count > 0 {
            Some(self.bids[0])
        } else {
            None
        }
    }

    /// Get the best ask price and quantity, if available.
    #[inline]
    pub fn best_ask(&self) -> Option<PriceLevel> {
        if self.ask_count > 0 {
            Some(self.asks[0])
        } else {
            None
        }
    }

    /// Get the best bid price, if available.
    #[inline]
    pub fn best_bid_price(&self) -> Option<f64> {
        self.best_bid().map(|l| l.price)
    }

    /// Get the best ask price, if available.
    #[inline]
    pub fn best_ask_price(&self) -> Option<f64> {
        self.best_ask().map(|l| l.price)
    }

    /// Calculate the bid-ask spread.
    #[inline]
    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid_price(), self.best_ask_price()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    /// Calculate the mid price.
    #[inline]
    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_bid_price(), self.best_ask_price()) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }

    /// Calculate the spread as a percentage of the mid price.
    #[inline]
    pub fn spread_bps(&self) -> Option<f64> {
        match (self.spread(), self.mid_price()) {
            (Some(spread), Some(mid)) if mid > 0.0 => Some(spread / mid * 10000.0),
            _ => None,
        }
    }

    /// Get bid levels as a slice.
    #[inline]
    pub fn bid_levels(&self) -> &[PriceLevel] {
        &self.bids[..self.bid_count as usize]
    }

    /// Get ask levels as a slice.
    #[inline]
    pub fn ask_levels(&self) -> &[PriceLevel] {
        &self.asks[..self.ask_count as usize]
    }

    /// Calculate total bid volume across all levels.
    pub fn total_bid_volume(&self) -> f64 {
        self.bid_levels().iter().map(|l| l.quantity).sum()
    }

    /// Calculate total ask volume across all levels.
    pub fn total_ask_volume(&self) -> f64 {
        self.ask_levels().iter().map(|l| l.quantity).sum()
    }

    /// Calculate volume imbalance: (bid_volume - ask_volume) / (bid_volume + ask_volume)
    pub fn volume_imbalance(&self) -> Option<f64> {
        let bid_vol = self.total_bid_volume();
        let ask_vol = self.total_ask_volume();
        let total = bid_vol + ask_vol;
        if total > 0.0 {
            Some((bid_vol - ask_vol) / total)
        } else {
            None
        }
    }

    /// Check if the orderbook is valid (has both bids and asks, not crossed).
    pub fn is_valid(&self) -> bool {
        if self.bid_count == 0 || self.ask_count == 0 {
            return false;
        }

        // Check for crossed book
        if let (Some(bid), Some(ask)) = (self.best_bid_price(), self.best_ask_price()) {
            if bid >= ask {
                return false;
            }
        }

        true
    }

    /// Validate orderbook integrity and return any issues found.
    ///
    /// Checks:
    /// 1. Bids are sorted descending (best bid first)
    /// 2. Asks are sorted ascending (best ask first)
    /// 3. No crossed book (best_bid < best_ask)
    /// 4. No zero or negative prices
    pub fn validate(&self) -> Result<(), String> {
        if self.bid_count == 0 || self.ask_count == 0
            || self.bid_count as usize > MAX_LEVELS || self.ask_count as usize > MAX_LEVELS {
            return Err("empty or oversized orderbook".into());
        }
        for level in self.bid_levels().iter().chain(self.ask_levels().iter()) {
            if !level.price.is_finite() || level.price <= 0.0
                || !level.quantity.is_finite() || level.quantity <= 0.0 {
                return Err("invalid orderbook price or quantity".into());
            }
        }
        // Check bids are descending
        let bid_levels = self.bid_levels();
        for i in 1..bid_levels.len() {
            if bid_levels[i].price > bid_levels[i - 1].price {
                return Err(format!(
                    "Bids not sorted descending: bid[{}]={:.2} > bid[{}]={:.2}",
                    i, bid_levels[i].price, i - 1, bid_levels[i - 1].price
                ));
            }
            if bid_levels[i].price <= 0.0 {
                return Err(format!("Invalid bid price at level {}: {:.2}", i, bid_levels[i].price));
            }
        }

        // Check asks are ascending
        let ask_levels = self.ask_levels();
        for i in 1..ask_levels.len() {
            if ask_levels[i].price < ask_levels[i - 1].price {
                return Err(format!(
                    "Asks not sorted ascending: ask[{}]={:.2} < ask[{}]={:.2}",
                    i, ask_levels[i].price, i - 1, ask_levels[i - 1].price
                ));
            }
            if ask_levels[i].price <= 0.0 {
                return Err(format!("Invalid ask price at level {}: {:.2}", i, ask_levels[i].price));
            }
        }

        // Check for crossed book
        if let (Some(bid), Some(ask)) = (self.best_bid_price(), self.best_ask_price()) {
            if bid >= ask {
                return Err(format!(
                    "Crossed book: best_bid={:.2} >= best_ask={:.2}",
                    bid, ask
                ));
            }
        }

        Ok(())
    }

    /// Debug helper: dump first N levels of the orderbook.
    pub fn debug_levels(&self, n: usize) -> String {
        let mut s = String::new();
        s.push_str(&format!("Bids ({} levels): ", self.bid_count));
        for (i, level) in self.bid_levels().iter().take(n).enumerate() {
            if i > 0 { s.push_str(", "); }
            s.push_str(&format!("{:.2}@{:.4}", level.price, level.quantity));
        }
        s.push_str(&format!(" | Asks ({} levels): ", self.ask_count));
        for (i, level) in self.ask_levels().iter().take(n).enumerate() {
            if i > 0 { s.push_str(", "); }
            s.push_str(&format!("{:.2}@{:.4}", level.price, level.quantity));
        }
        s
    }

    /// Set bid levels from a slice, sorting by price descending.
    ///
    /// Uses in-place sorting on fixed arrays to avoid heap allocation.
    pub fn set_bids(&mut self, levels: &[(f64, f64)], max_levels: usize) {
        // Copy valid levels directly into fixed array (no allocation)
        let mut count = 0;
        let limit = max_levels.min(MAX_LEVELS);
        for (price, qty) in levels.iter() {
            if *price > 0.0 && *qty > 0.0 && count < limit {
                self.bids[count] = PriceLevel::new(*price, *qty);
                count += 1;
            }
        }
        self.bid_count = count as u8;

        // Sort in-place (descending by price for bids)
        self.bids[..count].sort_by(|a, b| {
            b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Zero out remaining levels
        for i in count..MAX_LEVELS {
            self.bids[i] = PriceLevel::default();
        }
    }

    /// Set ask levels from a slice, sorting by price ascending.
    ///
    /// Uses in-place sorting on fixed arrays to avoid heap allocation.
    pub fn set_asks(&mut self, levels: &[(f64, f64)], max_levels: usize) {
        // Copy valid levels directly into fixed array (no allocation)
        let mut count = 0;
        let limit = max_levels.min(MAX_LEVELS);
        for (price, qty) in levels.iter() {
            if *price > 0.0 && *qty > 0.0 && count < limit {
                self.asks[count] = PriceLevel::new(*price, *qty);
                count += 1;
            }
        }
        self.ask_count = count as u8;

        // Sort in-place (ascending by price for asks)
        self.asks[..count].sort_by(|a, b| {
            a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Zero out remaining levels
        for i in count..MAX_LEVELS {
            self.asks[i] = PriceLevel::default();
        }
    }

    /// Parse `[price, qty]` string pairs once and keep the best `max_levels`
    /// best-first. Either feed direction is accepted; unsorted input is sorted.
    /// Any unparsable, nonfinite, or non-positive level rejects the whole book.
    pub fn set_levels_from_strings<S: AsRef<str>>(
        &mut self,
        is_bid: bool,
        levels: &[(S, S)],
        max_levels: usize,
    ) -> Result<(), String> {
        let better = |a: f64, b: f64| if is_bid { a > b } else { a < b };
        let price_of = |level: &(S, S)| fast_float::parse::<f64, _>(level.0.as_ref()).unwrap_or(0.0);
        // Oversized books: keep whichever end holds the best prices.
        let take = levels.len().min(MAX_LEVELS);
        let worst_first = levels.len() > MAX_LEVELS && better(price_of(&levels[levels.len() - 1]), price_of(&levels[0]));
        let source = if worst_first { &levels[levels.len() - take..] } else { &levels[..take] };

        let mut parsed = [PriceLevel::default(); MAX_LEVELS];
        for (slot, (price, qty)) in parsed.iter_mut().zip(source) {
            let (price, qty) = (price.as_ref(), qty.as_ref());
            let p = fast_float::parse::<f64, _>(price).ok().filter(|p| p.is_finite() && *p > 0.0)
                .ok_or_else(|| format!("invalid price {price:?}"))?;
            let q = fast_float::parse::<f64, _>(qty).ok().filter(|q| q.is_finite() && *q > 0.0)
                .ok_or_else(|| format!("invalid quantity {qty:?}"))?;
            *slot = PriceLevel::new(p, q);
        }
        let parsed = &mut parsed[..take];
        if parsed.windows(2).all(|w| better(w[1].price, w[0].price)) {
            parsed.reverse();
        } else if !parsed.windows(2).all(|w| !better(w[1].price, w[0].price)) {
            parsed.sort_by(|a, b| match (better(a.price, b.price), better(b.price, a.price)) {
                (true, _) => std::cmp::Ordering::Less,
                (_, true) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            });
        }

        let count = take.min(max_levels);
        let out = if is_bid { &mut self.bids } else { &mut self.asks };
        out[..count].copy_from_slice(&parsed[..count]);
        out[count..].fill(PriceLevel::default());
        if is_bid { self.bid_count = count as u8 } else { self.ask_count = count as u8 }
        Ok(())
    }
}

impl fmt::Debug for OrderbookSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderbookSnapshot")
            .field("symbol", &self.symbol)
            .field("sequence", &self.sequence)
            .field("bid_count", &self.bid_count)
            .field("ask_count", &self.ask_count)
            .field("best_bid", &self.best_bid_price())
            .field("best_ask", &self.best_ask_price())
            .field("spread", &self.spread())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SYMBOL: &str = "TST-USD";

    #[test]
    fn test_price_level() {
        let level = PriceLevel::new(100.0, 1.5);
        assert_eq!(level.notional(), 150.0);
        assert!(!level.is_empty());

        let empty = PriceLevel::default();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_symbol() {
        let symbol = Symbol::new(TEST_SYMBOL);
        assert_eq!(symbol.as_str(), TEST_SYMBOL);
        assert_eq!(symbol.len(), 7);

        // Test truncation
        let long = Symbol::new("VERYLONGSYMBOLNAME");
        assert_eq!(long.len(), 15);
    }

    #[test]
    fn test_orderbook_snapshot() {
        let mut ob = OrderbookSnapshot::new(Symbol::new(TEST_SYMBOL));

        ob.set_bids(&[(100.0, 1.0), (99.0, 2.0), (98.0, 3.0)], 20);
        ob.set_asks(&[(101.0, 1.0), (102.0, 2.0), (103.0, 3.0)], 20);

        assert_eq!(ob.best_bid_price(), Some(100.0));
        assert_eq!(ob.best_ask_price(), Some(101.0));
        assert_eq!(ob.spread(), Some(1.0));
        assert_eq!(ob.mid_price(), Some(100.5));
        assert!(ob.is_valid());
    }

    #[test]
    fn symbol_truncates_only_at_utf8_boundary() {
        let symbol = Symbol::new("12345678901234é");
        assert_eq!(symbol.as_str(), "12345678901234");
        assert!(Symbol::try_new("12345678901234é").is_err());
        assert_eq!(Symbol::try_new("BTC-USD").unwrap().as_str(), "BTC-USD");
    }

    #[test]
    fn levels_from_strings_keep_best_levels_in_either_feed_direction() {
        let s = |v: &[(&str, &str)]| v.iter().map(|(p, q)| (p.to_string(), q.to_string())).collect::<Vec<_>>();
        let mut ob = OrderbookSnapshot::new(Symbol::new(TEST_SYMBOL));
        ob.set_levels_from_strings(true, &s(&[("98.0", "1"), ("99.0", "1"), ("100.0", "1")]), 2).unwrap();
        ob.set_levels_from_strings(false, &s(&[("103.0", "1"), ("102.0", "1"), ("101.0", "1")]), 2).unwrap();
        assert_eq!((ob.bid_levels()[0].price, ob.bid_levels()[1].price, ob.bid_count), (100.0, 99.0, 2));
        assert_eq!((ob.ask_levels()[0].price, ob.ask_levels()[1].price, ob.ask_count), (101.0, 102.0, 2));
        ob.set_levels_from_strings(true, &s(&[("99.0", "1"), ("101.0", "1"), ("100.0", "1")]), 2).unwrap();
        ob.set_levels_from_strings(false, &s(&[("102.0", "1"), ("100.0", "1"), ("101.0", "1")]), 2).unwrap();
        assert_eq!((ob.bid_levels()[0].price, ob.bid_levels()[1].price), (101.0, 100.0));
        assert_eq!((ob.ask_levels()[0].price, ob.ask_levels()[1].price), (100.0, 101.0));
        assert!(ob.set_levels_from_strings(true, &s(&[("100.0", "0")]), 2).is_err());
        assert!(ob.set_levels_from_strings(false, &s(&[("nan", "1")]), 2).is_err());
        assert!(ob.set_levels_from_strings(true, &s(&[("-1", "1")]), 2).is_err());
    }
}
