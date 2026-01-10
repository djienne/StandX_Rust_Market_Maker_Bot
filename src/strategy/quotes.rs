//! Quote output and formatting.
//!
//! Provides quote structure and display formatting including
//! spread, bid/ask prices, and distances in basis points.

use crate::types::Symbol;
use crate::log_quote;
use std::time::Instant;

/// A calculated market making quote.
#[derive(Debug, Clone)]
pub struct Quote {
    /// Symbol being quoted
    pub symbol: Symbol,
    /// Bid price
    pub bid_price: f64,
    /// Ask price
    pub ask_price: f64,
    /// Order quantity in base asset
    pub quantity: f64,
    /// Mid price (reference)
    pub mid_price: f64,
    /// Spread (ask - bid)
    pub spread: f64,
    /// Current volatility (per-second, in ticks)
    pub volatility: f64,
    /// Current alpha (imbalance z-score)
    pub alpha: f64,
    /// Current position in base asset
    pub position: f64,
    /// Half-spread in ticks
    pub half_spread_tick: f64,
    /// Whether this quote is valid for trading (has enough history)
    pub valid_for_trading: bool,
    /// History duration in seconds
    pub history_secs: f64,
    /// Whether bid depth was floored to minimum spread
    pub bid_floored: bool,
    /// Whether ask depth was floored to minimum spread
    pub ask_floored: bool,
}

impl Quote {
    /// Calculate bid distance from mid in basis points.
    pub fn bid_bps(&self) -> f64 {
        if self.mid_price == 0.0 {
            return 0.0;
        }
        ((self.bid_price - self.mid_price) / self.mid_price) * 10000.0
    }

    /// Calculate ask distance from mid in basis points.
    pub fn ask_bps(&self) -> f64 {
        if self.mid_price == 0.0 {
            return 0.0;
        }
        ((self.ask_price - self.mid_price) / self.mid_price) * 10000.0
    }

    /// Calculate spread in basis points.
    pub fn spread_bps(&self) -> f64 {
        if self.mid_price == 0.0 {
            return 0.0;
        }
        (self.spread / self.mid_price) * 10000.0
    }

    /// Get half-spread in basis points.
    pub fn half_spread_bps(&self) -> f64 {
        self.spread_bps() / 2.0
    }
}

/// Quote formatter for display output with throttling.
pub struct QuoteFormatter {
    /// Price precision (decimal places)
    price_precision: usize,
    /// Quantity precision (decimal places)
    qty_precision: usize,
    /// Minimum interval between logs in milliseconds
    log_interval_ms: u64,
    /// Last time a quote was logged
    last_log_time: Option<Instant>,
}

impl Default for QuoteFormatter {
    fn default() -> Self {
        Self {
            price_precision: 2,
            qty_precision: 4,
            log_interval_ms: 1000, // Log at most once per second
            last_log_time: None,
        }
    }
}

impl QuoteFormatter {
    /// Create a new formatter with specified precision.
    pub fn new(price_precision: usize, qty_precision: usize) -> Self {
        Self {
            price_precision,
            qty_precision,
            log_interval_ms: 1000,
            last_log_time: None,
        }
    }

    /// Set the minimum interval between quote logs in milliseconds.
    pub fn with_log_interval(mut self, interval_ms: u64) -> Self {
        self.log_interval_ms = interval_ms;
        self
    }

    /// Format and log a quote (throttled to log_interval_ms).
    pub fn log_quote(&mut self, quote: &Quote) {
        // Check throttle
        let now = Instant::now();
        if let Some(last) = self.last_log_time {
            if now.duration_since(last).as_millis() < self.log_interval_ms as u128 {
                return; // Skip logging, too soon
            }
        }
        self.last_log_time = Some(now);

        // Trading status indicator
        let trade_status = if quote.valid_for_trading {
            "READY"
        } else {
            "WARM"
        };

        // Header line: symbol, mid, spread, volatility, alpha, status
        log_quote!(
            "[{}] mid={:.prec$} spread={:.prec$} ({:.2}bps) vol={:.4} alpha={:.3} [{} {:.0}s]",
            quote.symbol.as_str(),
            quote.mid_price,
            quote.spread,
            quote.spread_bps(),
            quote.volatility,
            quote.alpha,
            trade_status,
            quote.history_secs,
            prec = self.price_precision
        );

        // Quote line: bid and ask with bps (show [FLOOR] if minimum was applied)
        let bid_floor_indicator = if quote.bid_floored { " [FLOOR]" } else { "" };
        let ask_floor_indicator = if quote.ask_floored { " [FLOOR]" } else { "" };
        log_quote!(
            "  Quote: bid={:.prec$} ({:+.2}bps){} ask={:.prec$} ({:+.2}bps){} qty={:.qprec$}",
            quote.bid_price,
            quote.bid_bps(),
            bid_floor_indicator,
            quote.ask_price,
            quote.ask_bps(),
            ask_floor_indicator,
            quote.quantity,
            prec = self.price_precision,
            qprec = self.qty_precision
        );

        // Position info if non-zero
        if quote.position.abs() > 1e-8 {
            log_quote!(
                "  Position: {:.qprec$} (${:.2})",
                quote.position,
                quote.position * quote.mid_price,
                qprec = self.qty_precision
            );
        }
    }

    /// Format quote as a single-line string.
    pub fn format_oneline(&self, quote: &Quote) -> String {
        format!(
            "[{}] bid={:.prec$}({:+.1}bp) ask={:.prec$}({:+.1}bp) spread={:.1}bp vol={:.3} alpha={:.2}",
            quote.symbol.as_str(),
            quote.bid_price,
            quote.bid_bps(),
            quote.ask_price,
            quote.ask_bps(),
            quote.spread_bps(),
            quote.volatility,
            quote.alpha,
            prec = self.price_precision
        )
    }

    /// Format quote as multi-line string.
    pub fn format_multiline(&self, quote: &Quote) -> String {
        let mut output = String::new();

        // Header
        output.push_str(&format!(
            "[{}] mid={:.prec$} spread={:.prec$} ({:.2}bps)\n",
            quote.symbol.as_str(),
            quote.mid_price,
            quote.spread,
            quote.spread_bps(),
            prec = self.price_precision
        ));

        // Strategy metrics
        output.push_str(&format!(
            "  Metrics: vol={:.4} alpha={:.3} half_spread={:.2}ticks\n",
            quote.volatility,
            quote.alpha,
            quote.half_spread_tick
        ));

        // Quote prices
        output.push_str(&format!(
            "  Quote: bid={:.prec$} ({:+.2}bps) | ask={:.prec$} ({:+.2}bps)\n",
            quote.bid_price,
            quote.bid_bps(),
            quote.ask_price,
            quote.ask_bps(),
            prec = self.price_precision
        ));

        // Quantity
        output.push_str(&format!(
            "  Size: {:.qprec$} (${:.2})\n",
            quote.quantity,
            quote.quantity * quote.mid_price,
            qprec = self.qty_precision
        ));

        // Position if non-zero
        if quote.position.abs() > 1e-8 {
            output.push_str(&format!(
                "  Position: {:.qprec$} (${:.2})\n",
                quote.position,
                quote.position * quote.mid_price,
                qprec = self.qty_precision
            ));
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_quote() -> Quote {
        Quote {
            symbol: Symbol::new("BTC-USD"),
            bid_price: 99999.50,
            ask_price: 100000.50,
            quantity: 0.001,
            mid_price: 100000.0,
            spread: 1.0,
            volatility: 0.0234,
            alpha: 0.15,
            position: 0.0,
            half_spread_tick: 50.0,
            valid_for_trading: true,
            history_secs: 600.0,
            bid_floored: false,
            ask_floored: false,
        }
    }

    #[test]
    fn test_bid_bps() {
        let quote = sample_quote();
        // (99999.50 - 100000) / 100000 * 10000 = -0.05 bps
        assert!((quote.bid_bps() - (-0.05)).abs() < 0.001);
    }

    #[test]
    fn test_ask_bps() {
        let quote = sample_quote();
        // (100000.50 - 100000) / 100000 * 10000 = 0.05 bps
        assert!((quote.ask_bps() - 0.05).abs() < 0.001);
    }

    #[test]
    fn test_spread_bps() {
        let quote = sample_quote();
        // 1.0 / 100000 * 10000 = 0.1 bps
        assert!((quote.spread_bps() - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_format_oneline() {
        let quote = sample_quote();
        let formatter = QuoteFormatter::default();
        let output = formatter.format_oneline(&quote);

        assert!(output.contains("BTC-USD"));
        assert!(output.contains("bid="));
        assert!(output.contains("ask="));
        assert!(output.contains("bp"));
    }

    #[test]
    fn test_format_multiline() {
        let quote = sample_quote();
        let formatter = QuoteFormatter::default();
        let output = formatter.format_multiline(&quote);

        assert!(output.contains("BTC-USD"));
        assert!(output.contains("mid="));
        assert!(output.contains("Quote:"));
        assert!(output.contains("Size:"));
    }
}
