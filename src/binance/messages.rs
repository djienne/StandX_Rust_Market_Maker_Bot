//! Binance WebSocket and REST API message parsing.
//!
//! This module handles deserialization of Binance messages:
//! - Depth (orderbook) REST snapshots and WebSocket incremental updates
//! - BookTicker (best bid/offer) WebSocket updates

use serde::Deserialize;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;

/// Errors that can occur during message parsing.
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Failed to parse JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid message type: expected text")]
    InvalidMessageType,

    #[error("Failed to parse price: {0}")]
    InvalidPrice(String),

    #[error("Failed to parse quantity: {0}")]
    InvalidQuantity(String),
}

/// Binance REST API depth snapshot response.
///
/// Fetched from: `GET /api/v3/depth?symbol=BTCUSDT&limit=1000`
///
/// Example response:
/// ```json
/// {
///   "lastUpdateId": 1027024,
///   "bids": [["4.00000000", "431.00000000"]],
///   "asks": [["4.00000200", "12.00000000"]]
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceDepthSnapshot {
    /// Last update ID for sequence synchronization
    pub last_update_id: u64,

    /// Bid levels as [price, quantity] string pairs
    pub bids: Vec<[String; 2]>,

    /// Ask levels as [price, quantity] string pairs
    pub asks: Vec<[String; 2]>,
}

/// Binance WebSocket depth update event.
///
/// Works for both Spot and Futures streams.
///
/// Spot example (`wss://stream.binance.com:9443/ws/btcusdt@depth@100ms`):
/// ```json
/// {
///   "e": "depthUpdate",
///   "E": 123456789,
///   "s": "BTCUSDT",
///   "U": 157,
///   "u": 160,
///   "b": [["0.0024", "10"]],
///   "a": [["0.0026", "100"]]
/// }
/// ```
///
/// Futures example (`wss://fstream.binance.com/ws/btcusdt@depth@100ms`):
/// ```json
/// {
///   "e": "depthUpdate",
///   "E": 123456789,
///   "T": 123456788,
///   "s": "BTCUSDT",
///   "U": 157,
///   "u": 160,
///   "pu": 156,
///   "b": [["0.0024", "10"]],
///   "a": [["0.0026", "100"]]
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceDepthUpdate {
    /// Event type (should be "depthUpdate")
    #[serde(rename = "e")]
    pub event_type: String,

    /// Event time (milliseconds since epoch)
    #[serde(rename = "E")]
    pub event_time: u64,

    /// Symbol
    #[serde(rename = "s")]
    pub symbol: String,

    /// First update ID in event
    #[serde(rename = "U")]
    pub first_update_id: u64,

    /// Final update ID in event
    #[serde(rename = "u")]
    pub final_update_id: u64,

    /// Previous final update ID (Futures only)
    /// Used for continuity checking in Futures streams
    #[serde(rename = "pu")]
    pub prev_final_update_id: Option<u64>,

    /// Bid delta levels as [price, quantity] string pairs
    /// Quantity of 0 means remove the level
    #[serde(rename = "b")]
    pub bids: Vec<[String; 2]>,

    /// Ask delta levels as [price, quantity] string pairs
    /// Quantity of 0 means remove the level
    #[serde(rename = "a")]
    pub asks: Vec<[String; 2]>,
}

impl BinanceDepthUpdate {
    /// Check if this is a Futures update (has `pu` field).
    pub fn is_futures(&self) -> bool {
        self.prev_final_update_id.is_some()
    }
}

/// Parse a WebSocket message into a depth update.
///
/// Returns `None` for ping/pong/close frames (handled by tungstenite).
pub fn parse_depth_update(msg: &Message) -> Result<Option<BinanceDepthUpdate>, ParseError> {
    match msg {
        Message::Text(text) => {
            let update: BinanceDepthUpdate = serde_json::from_str(text)?;
            Ok(Some(update))
        }
        Message::Binary(data) => {
            let update: BinanceDepthUpdate = serde_json::from_slice(data)?;
            Ok(Some(update))
        }
        // Ping/Pong/Close are handled by tokio-tungstenite
        Message::Ping(_) | Message::Pong(_) | Message::Close(_) | Message::Frame(_) => Ok(None),
    }
}

/// Parse a REST API response into a depth snapshot.
pub fn parse_snapshot(json: &str) -> Result<BinanceDepthSnapshot, ParseError> {
    let snapshot: BinanceDepthSnapshot = serde_json::from_str(json)?;
    Ok(snapshot)
}

/// Binance Futures bookTicker WebSocket event.
///
/// Pushed in real-time on every change to best bid/ask price or quantity.
/// Stream: `wss://fstream.binance.com/ws/btcusdt@bookTicker`
///
/// Example (captured from live stream):
/// ```json
/// {
///   "e": "bookTicker",
///   "u": 10206941216683,
///   "s": "BTCUSDT",
///   "b": "66268.70",
///   "B": "9.313",
///   "a": "66268.80",
///   "A": "5.084",
///   "T": 1774691007303,
///   "E": 1774691007303
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceBookTicker {
    /// Event type (should be "bookTicker")
    #[serde(rename = "e")]
    pub event_type: String,

    /// Order book update ID
    #[serde(rename = "u")]
    pub update_id: u64,

    /// Symbol
    #[serde(rename = "s")]
    pub symbol: String,

    /// Best bid price (string)
    #[serde(rename = "b")]
    pub best_bid_price: String,

    /// Best bid quantity (string)
    #[serde(rename = "B")]
    pub best_bid_qty: String,

    /// Best ask price (string)
    #[serde(rename = "a")]
    pub best_ask_price: String,

    /// Best ask quantity (string)
    #[serde(rename = "A")]
    pub best_ask_qty: String,

    /// Transaction time (milliseconds since epoch)
    #[serde(rename = "T")]
    pub transaction_time: u64,

    /// Event time (milliseconds since epoch)
    #[serde(rename = "E")]
    pub event_time: u64,
}

impl BinanceBookTicker {
    /// Parse best bid price to f64.
    #[inline]
    pub fn bid_price_f64(&self) -> Result<f64, ParseError> {
        parse_price_str(&self.best_bid_price)
    }

    /// Parse best ask price to f64.
    #[inline]
    pub fn ask_price_f64(&self) -> Result<f64, ParseError> {
        parse_price_str(&self.best_ask_price)
    }

    /// Parse best bid quantity to f64.
    #[inline]
    pub fn bid_qty_f64(&self) -> Result<f64, ParseError> {
        parse_qty_str(&self.best_bid_qty)
    }

    /// Parse best ask quantity to f64.
    #[inline]
    pub fn ask_qty_f64(&self) -> Result<f64, ParseError> {
        parse_qty_str(&self.best_ask_qty)
    }
}

/// Parse a WebSocket message into a bookTicker update.
///
/// Returns `None` for ping/pong/close frames.
pub fn parse_book_ticker(msg: &Message) -> Result<Option<BinanceBookTicker>, ParseError> {
    match msg {
        Message::Text(text) => {
            let ticker: BinanceBookTicker = serde_json::from_str(text)?;
            Ok(Some(ticker))
        }
        Message::Binary(data) => {
            let ticker: BinanceBookTicker = serde_json::from_slice(data)?;
            Ok(Some(ticker))
        }
        Message::Ping(_) | Message::Pong(_) | Message::Close(_) | Message::Frame(_) => Ok(None),
    }
}

/// Parse a price string to f64 using fast_float.
#[inline]
fn parse_price_str(s: &str) -> Result<f64, ParseError> {
    let v: f64 = fast_float::parse(s).map_err(|_| ParseError::InvalidPrice(s.to_string()))?;
    if !v.is_finite() {
        return Err(ParseError::InvalidPrice(s.to_string()));
    }
    Ok(v)
}

/// Parse a quantity string to f64 using fast_float.
#[inline]
fn parse_qty_str(s: &str) -> Result<f64, ParseError> {
    let v: f64 = fast_float::parse(s).map_err(|_| ParseError::InvalidQuantity(s.to_string()))?;
    if !v.is_finite() {
        return Err(ParseError::InvalidQuantity(s.to_string()));
    }
    Ok(v)
}

/// Parse a price/quantity string pair into f64 values.
///
/// Uses fast-float for high-performance parsing.
#[inline]
pub fn parse_level(level: &[String; 2]) -> Result<(f64, f64), ParseError> {
    let price: f64 = fast_float::parse(&level[0])
        .map_err(|_| ParseError::InvalidPrice(level[0].clone()))?;
    let qty: f64 = fast_float::parse(&level[1])
        .map_err(|_| ParseError::InvalidQuantity(level[1].clone()))?;
    // Reject NaN/Infinity from malformed inputs (fast_float can parse "NaN"/"Infinity")
    if !price.is_finite() {
        return Err(ParseError::InvalidPrice(level[0].clone()));
    }
    if !qty.is_finite() {
        return Err(ParseError::InvalidQuantity(level[1].clone()));
    }
    Ok((price, qty))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_depth_snapshot() {
        let json = r#"{
            "lastUpdateId": 1027024,
            "bids": [
                ["4.00000000", "431.00000000"],
                ["3.99000000", "100.00000000"]
            ],
            "asks": [
                ["4.00000200", "12.00000000"],
                ["5.00000000", "50.00000000"]
            ]
        }"#;

        let snapshot = parse_snapshot(json).unwrap();
        assert_eq!(snapshot.last_update_id, 1027024);
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 2);
        assert_eq!(snapshot.bids[0][0], "4.00000000");
        assert_eq!(snapshot.bids[0][1], "431.00000000");
    }

    #[test]
    fn test_parse_depth_update() {
        let json = r#"{
            "e": "depthUpdate",
            "E": 123456789,
            "s": "BTCUSDT",
            "U": 157,
            "u": 160,
            "b": [["0.0024", "10"]],
            "a": [["0.0026", "100"]]
        }"#;

        let msg = Message::Text(json.to_string());
        let update = parse_depth_update(&msg).unwrap().unwrap();

        assert_eq!(update.event_type, "depthUpdate");
        assert_eq!(update.event_time, 123456789);
        assert_eq!(update.symbol, "BTCUSDT");
        assert_eq!(update.first_update_id, 157);
        assert_eq!(update.final_update_id, 160);
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.asks.len(), 1);
    }

    #[test]
    fn test_parse_level() {
        let level = ["100.50".to_string(), "1.25".to_string()];
        let (price, qty) = parse_level(&level).unwrap();
        assert!((price - 100.50).abs() < 1e-10);
        assert!((qty - 1.25).abs() < 1e-10);
    }

    #[test]
    fn test_parse_ping_returns_none() {
        let msg = Message::Ping(vec![]);
        assert!(parse_depth_update(&msg).unwrap().is_none());
    }

    #[test]
    fn test_parse_book_ticker_real_captured() {
        // Real JSON captured from wss://fstream.binance.com/ws/btcusdt@bookTicker
        let json = r#"{"e":"bookTicker","u":10206941216683,"s":"BTCUSDT","b":"66268.70","B":"9.313","a":"66268.80","A":"5.084","T":1774691007303,"E":1774691007303}"#;

        let msg = Message::Text(json.to_string());
        let ticker = parse_book_ticker(&msg).unwrap().unwrap();

        assert_eq!(ticker.event_type, "bookTicker");
        assert_eq!(ticker.update_id, 10206941216683);
        assert_eq!(ticker.symbol, "BTCUSDT");
        assert_eq!(ticker.best_bid_price, "66268.70");
        assert_eq!(ticker.best_bid_qty, "9.313");
        assert_eq!(ticker.best_ask_price, "66268.80");
        assert_eq!(ticker.best_ask_qty, "5.084");
        assert_eq!(ticker.transaction_time, 1774691007303);
        assert_eq!(ticker.event_time, 1774691007303);

        // Test f64 parsing
        assert!((ticker.bid_price_f64().unwrap() - 66268.70).abs() < 1e-10);
        assert!((ticker.ask_price_f64().unwrap() - 66268.80).abs() < 1e-10);
        assert!((ticker.bid_qty_f64().unwrap() - 9.313).abs() < 1e-10);
        assert!((ticker.ask_qty_f64().unwrap() - 5.084).abs() < 1e-10);
    }

    #[test]
    fn test_parse_book_ticker_ping_returns_none() {
        let msg = Message::Ping(vec![]);
        assert!(parse_book_ticker(&msg).unwrap().is_none());
    }
}
