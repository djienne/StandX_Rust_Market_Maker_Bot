//! StandX BTC market maker. See README.md for operation and SPREAD_CALCULATION.md for the model.

pub mod binance;
pub mod config;
pub mod logging;
pub mod orderbook;
pub mod strategy;
pub mod trading;
pub mod types;
pub mod websocket;

// Re-export commonly used types
pub use config::{Config, ConfigError, WebSocketConfig, StrategyConfig, PositionConfig, OrderConfig, WalletConfig, SanityCheckConfig, SymbolInfoConfig};
pub use logging::{init as init_logging, is_enabled as logging_enabled, logger};
pub use orderbook::{
    CurrentOrderbook, OrderbookManager, OrderbookStore,
    SymbolOrderbook, OrderbookStats,
    OrderbookSanityChecker, SanityCheckerConfig, SanityCheckerHandle, SanityCheckerStats,
};
pub use strategy::{QuoteStrategy, ObiStrategy, Quote, QuoteFormatter, RollingStats, RollingWindow};
pub use trading::{
    AuthManager, SharedPosition, PositionPoller, PositionPollerConfig, PositionPollerHandle,
    QuoteOrderManager, OrderManagerConfig, OrderDecision, OrderDecisions, LiveOrder, OrderState, PauseReason, Side, OrderManagerStats,
    WalletTracker, WalletTrackerConfig, WalletTrackerHandle,
    OpenOrdersChecker, OpenOrdersCheckerConfig, OpenOrdersCheckerHandle, ClearOrdersSignal,
    OpenOrdersSnapshot,
    SharedSymbolInfo, SymbolInfoPoller, SymbolInfoPollerConfig, SymbolInfoPollerHandle, TickSizeChangedSignal,
};
pub use types::{OrderbookSnapshot, PriceLevel, Symbol, SymbolError, MAX_LEVELS};
pub use binance::{
    BinanceClient, BinanceClientConfig, BinanceEvent, BinanceObiCalculator,
    BinanceOrderbook, BinanceWsStats, BinanceWsStatsSnapshot,
    SharedAlpha, BinanceAlphaPollerHandle, start_binance_alpha_poller,
};
pub use websocket::{
    WsClient, WsClientBuilder, WsEvent, WsStats, WsStatsSnapshot,
    StandXMessage, MessageError,
    current_time_ns,
};

/// Library version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default WebSocket URL for market data.
pub const WS_STREAM_URL: &str = "wss://perps.standx.com/ws-stream/v1";

/// Default WebSocket URL for orders.
pub const WS_API_URL: &str = "wss://perps.standx.com/ws-api/v1";
