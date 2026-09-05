//! StandX WebSocket message parsing.
//!
//! This module handles deserialization of StandX market data messages
//! including orderbook depth, trades, and prices.

use std::borrow::Cow;

use chrono::DateTime;
use serde::Deserialize;
use serde_json::value::RawValue;
use thiserror::Error;

use crate::types::{OrderbookSnapshot, Symbol};

/// Message parsing errors.
#[derive(Error, Debug)]
pub enum MessageError {
    #[error("JSON parse error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Invalid price format: {0}")]
    InvalidPrice(String),

    #[error("Invalid quantity format: {0}")]
    InvalidQuantity(String),

    #[error("Invalid timestamp format: {0}")]
    InvalidTimestamp(String),

    #[error("Unknown channel: {0}")]
    UnknownChannel(String),

    #[error("Missing required field: {0}")]
    MissingField(String),
}

/// Envelope with the channel payload left unparsed until the channel is known.
#[derive(Deserialize)]
struct RawMessage<'a> {
    #[serde(borrow)]
    channel: Option<&'a str>,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
    code: Option<i32>,
    message: Option<String>,
}

/// Depth payload borrowed from the message text. Numbers are parsed once,
/// straight into the fixed-size snapshot.
#[derive(Deserialize)]
struct DepthBookData<'a> {
    #[serde(borrow)]
    symbol: Cow<'a, str>,
    #[serde(borrow)]
    asks: Vec<(&'a str, &'a str)>,
    #[serde(borrow)]
    bids: Vec<(&'a str, &'a str)>,
    sequence: Option<u64>,
    /// Integer milliseconds or an RFC 3339 string.
    #[serde(borrow, default)]
    time: Option<&'a RawValue>,
}

/// Price data from the price channel.
#[derive(Debug, Deserialize)]
pub struct PriceData {
    pub symbol: String,
    pub index_price: Option<String>,
    pub last_price: Option<String>,
    pub mark_price: Option<String>,
    pub mid_price: Option<String>,
    pub spread: Option<(String, String)>,
    pub time: Option<String>,
}

/// Trade data from the public_trade channel.
#[derive(Debug, Deserialize)]
pub struct TradeData {
    pub symbol: String,
    pub price: String,
    pub qty: String,
    pub quote_qty: Option<String>,
    pub is_buyer_taker: bool,
    pub time: Option<String>,
}

/// Parsed StandX message. The depth variant is inline by design (see `WsEvent`).
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum StandXMessage {
    /// Validated orderbook snapshot with the receive timestamp stamped
    DepthBook(OrderbookSnapshot),
    /// Price update
    Price(PriceData),
    /// Public trade
    Trade(TradeData),
    /// Authentication response
    Auth { code: i32, message: String },
    /// Error message
    Error { code: i32, message: String },
    /// Unknown or unhandled message
    Unknown(serde_json::Value),
}

impl StandXMessage {
    /// Parse a raw message. Depth payloads become a validated snapshot holding
    /// at most `max_levels` per side with `received_at_ns` stamped.
    pub fn parse(data: &[u8], max_levels: usize, received_at_ns: i64) -> Result<Self, MessageError> {
        let raw: RawMessage = serde_json::from_slice(data)?;
        let payload = || raw.data.ok_or_else(|| MessageError::MissingField("data".into()));
        match raw.channel {
            Some("depth_book") => depth_to_snapshot(payload()?, max_levels, received_at_ns).map(StandXMessage::DepthBook),
            Some("price") => Ok(StandXMessage::Price(serde_json::from_str(payload()?.get())?)),
            Some("public_trade") => Ok(StandXMessage::Trade(serde_json::from_str(payload()?.get())?)),
            Some("auth") => {
                let data: serde_json::Value = serde_json::from_str(payload()?.get())?;
                Ok(StandXMessage::Auth {
                    code: data.get("code").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
                    message: data.get("msg").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                })
            }
            _ => match (raw.channel, raw.code, raw.data) {
                (_, Some(code), _) if code != 0 => Ok(StandXMessage::Error { code, message: raw.message.unwrap_or_default() }),
                (Some(channel), _, None) => Err(MessageError::UnknownChannel(channel.to_string())),
                (_, _, Some(data)) => Ok(StandXMessage::Unknown(serde_json::from_str(data.get())?)),
                (_, _, None) => Ok(StandXMessage::Unknown(serde_json::from_slice(data)?)),
            },
        }
    }

    /// Parse from a string.
    pub fn parse_str(s: &str, max_levels: usize, received_at_ns: i64) -> Result<Self, MessageError> {
        Self::parse(s.as_bytes(), max_levels, received_at_ns)
    }
}

fn depth_to_snapshot(raw: &RawValue, max_levels: usize, received_at_ns: i64) -> Result<OrderbookSnapshot, MessageError> {
    let depth: DepthBookData = serde_json::from_str(raw.get())?;
    let mut snapshot = OrderbookSnapshot::new(Symbol::new(&depth.symbol));
    snapshot.set_levels_from_strings(true, &depth.bids, max_levels).map_err(MessageError::InvalidPrice)?;
    snapshot.set_levels_from_strings(false, &depth.asks, max_levels).map_err(MessageError::InvalidPrice)?;
    snapshot.sequence = depth.sequence.unwrap_or(0);
    if let Some(time) = depth.time {
        snapshot.timestamp_ns = parse_timestamp_raw(time.get())?;
    }
    snapshot.received_at_ns = received_at_ns;
    snapshot.validate().map_err(MessageError::InvalidPrice)?;
    Ok(snapshot)
}

/// Raw JSON timestamp: integer milliseconds or a quoted RFC 3339 string.
fn parse_timestamp_raw(raw: &str) -> Result<i64, MessageError> {
    match raw.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(text) => parse_timestamp(text),
        None => Ok(raw.parse::<i64>().map_or(0, |ms| ms.saturating_mul(1_000_000))),
    }
}

/// Parse ISO 8601 timestamp to nanoseconds since Unix epoch.
///
/// Optimized to avoid string allocation - uses stack buffer for 'Z' suffix.
#[inline]
pub fn parse_timestamp(s: &str) -> Result<i64, MessageError> {
    // Try parsing as ISO 8601 (handles both with and without 'Z')
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_nanos_opt().unwrap_or(0));
    }

    // If no 'Z' suffix, try appending it without heap allocation
    if !s.ends_with('Z') && s.len() < 64 {
        // Use stack buffer to avoid heap allocation
        let mut buf = [0u8; 64];
        let bytes = s.as_bytes();
        buf[..bytes.len()].copy_from_slice(bytes);
        buf[bytes.len()] = b'Z';

        if let Ok(s_with_z) = std::str::from_utf8(&buf[..bytes.len() + 1]) {
            if let Ok(dt) = DateTime::parse_from_rfc3339(s_with_z) {
                return Ok(dt.timestamp_nanos_opt().unwrap_or(0));
            }
        }
    }

    // Fallback - only allocate on error path (cold)
    Err(MessageError::InvalidTimestamp(s.to_string()))
}

/// Get current time in nanoseconds since Unix epoch.
pub fn current_time_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Create a subscription message for a channel.
pub fn subscribe_message(channel: &str, symbol: &str) -> String {
    serde_json::json!({
        "subscribe": {
            "channel": channel,
            "symbol": symbol
        }
    }).to_string()
}

/// Create an unsubscription message for a channel.
pub fn unsubscribe_message(channel: &str, symbol: &str) -> String {
    serde_json::json!({
        "unsubscribe": {
            "channel": channel,
            "symbol": symbol
        }
    }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_message_becomes_validated_snapshot_with_best_levels_first() {
        // Feed order matches DOCS/websocket.md: bids ascend, asks ascend.
        let json = r#"{"seq":1,"channel":"depth_book","data":{"symbol":"TEST-USD",
            "asks":[["101.00","1.0"],["102.00","2.0"],["103.00","3.0"]],
            "bids":[["98.00","2.0"],["99.00","2.0"],["100.00","1.0"]],
            "sequence":12345,"time":"2025-08-11T03:44:40.922233Z"}}"#;
        let StandXMessage::DepthBook(book) = StandXMessage::parse_str(json, 2, 77).unwrap() else { panic!("depth") };
        assert_eq!(book.symbol.as_str(), "TEST-USD");
        assert_eq!((book.bid_count, book.ask_count, book.sequence, book.received_at_ns), (2, 2, 12345, 77));
        assert_eq!((book.bids[0].price, book.bids[1].price), (100.0, 99.0));
        assert_eq!((book.asks[0].price, book.asks[1].quantity), (101.0, 2.0));
        assert_eq!(book.timestamp_ns, 1_754_883_880_922_233_000);
        let ms = json.replace(r#""2025-08-11T03:44:40.922233Z""#, "1754883880922");
        let StandXMessage::DepthBook(book) = StandXMessage::parse_str(&ms, 2, 0).unwrap() else { panic!("depth") };
        assert_eq!(book.timestamp_ns, 1_754_883_880_922_000_000);
        // Crossed, empty, and malformed books are rejected before they reach the strategy.
        for bad in [
            json.replace(r#"["100.00","1.0"]"#, r#"["101.50","1.0"]"#),
            json.replace(r#""bids":[["98.00","2.0"],["99.00","2.0"],["100.00","1.0"]]"#, r#""bids":[]"#),
            json.replace(r#"["102.00","2.0"]"#, r#"["102.00","-2.0"]"#),
            json.replace(r#"["102.00","2.0"]"#, r#"["1e999","2.0"]"#),
        ] {
            assert!(StandXMessage::parse_str(&bad, 2, 0).is_err(), "{bad}");
        }
    }

    #[test]
    fn test_parse_trade() {
        let json = r#"{
            "seq": 1,
            "channel": "public_trade",
            "data": {
                "symbol": "TEST-USD",
                "price": "121720.18",
                "qty": "0.01",
                "quote_qty": "1217.2018",
                "is_buyer_taker": true,
                "time": "2025-08-11T03:48:47.086505Z"
            }
        }"#;

        let msg = StandXMessage::parse_str(json, 20, 0).unwrap();
        match msg {
            StandXMessage::Trade(data) => {
                assert_eq!(data.symbol, "TEST-USD");
                assert_eq!(data.price, "121720.18");
                assert!(data.is_buyer_taker);
            }
            _ => panic!("Expected Trade message"),
        }
    }

    #[test]
    fn test_parse_error() {
        let json = r#"{
            "code": 400,
            "message": "Bad request"
        }"#;

        let msg = StandXMessage::parse_str(json, 20, 0).unwrap();
        match msg {
            StandXMessage::Error { code, message } => {
                assert_eq!(code, 400);
                assert_eq!(message, "Bad request");
            }
            _ => panic!("Expected Error message"),
        }
    }

    #[test]
    fn test_subscribe_message() {
        let msg: serde_json::Value = serde_json::from_str(&subscribe_message("depth_book", "TEST-USD")).unwrap();
        assert_eq!(msg["subscribe"], serde_json::json!({"channel": "depth_book", "symbol": "TEST-USD"}));
    }
}
