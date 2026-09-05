//! WebSocket client module for StandX market data.
//!
//! This module provides:
//! - [`WsClient`]: WebSocket client with automatic reconnection
//! - [`StandXMessage`]: Parsed message types
//! - Message parsing utilities
//! - [`ReconnectConfig`]: Shared reconnection configuration

mod client;
mod messages;
pub mod reconnect;

pub use client::{WsClient, WsClientBuilder, WsEvent, WsStats, WsStatsSnapshot};
pub use messages::{
    StandXMessage, PriceData, TradeData,
    MessageError, subscribe_message, unsubscribe_message,
    parse_timestamp, current_time_ns,
};
pub use reconnect::{ReconnectConfig, ReconnectState};

/// Process-local monotonic clock for depth freshness and strategy sampling.
pub fn monotonic_time_ns() -> i64 {
    static ORIGIN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    ORIGIN.get_or_init(std::time::Instant::now).elapsed().as_nanos() as i64 + 1
}
