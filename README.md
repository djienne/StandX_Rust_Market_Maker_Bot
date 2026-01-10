# StandX Market Maker

High-performance market making system for the StandX perpetual futures exchange, written in Rust.

## Features

- **Low-latency architecture**: Synchronous hot path (<10μs decisions), async execution
- **Lock-free data structures**: Triple buffer for orderbook, atomic position reads
- **OBI Strategy**: Order Book Imbalance based quotes with volatility scaling and position skew
- **Modular design**: `QuoteStrategy` trait for easy strategy swapping
- **WebSocket trading**: Low-latency order execution (5-50ms)
- **Position management**: Background polling with lock-free reads
- **Auto-reconnection**: Exponential backoff on disconnect
- **Graceful shutdown**: Cancels all orders on Ctrl+C

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Main Event Loop (tokio::select!)                  │
│                                                                      │
│  WS Orderbook ──► ObiStrategy ──► OrderManager.on_quote() ──► Tx    │
│                   .update()       (SYNC, <10μs)              (mpsc) │
│                      │                   │                     │    │
│                      ▼                   ▼                     ▼    │
│               SharedPosition      OrderDecision         OrderExecutor│
│               (lock-free read)                          (async task)│
└─────────────────────────────────────────────────────────────────────┘
                                                               │
┌──────────────────────────────────────────────────────────────┼──────┐
│                    Order Event Handler                       │      │
│                                                              ▼      │
│  OrderWsClient ──► OrderEvent (Accept/Cancel) ──► State Update     │
└─────────────────────────────────────────────────────────────────────┘
                                                               │
┌──────────────────────────────────────────────────────────────┼──────┐
│               Position Poller (HTTP, 2s interval)            │      │
│                                                              ▼      │
│  REST API ──► Position ──► SharedPosition (Atomic Update)          │
└─────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Rust 1.70+
- StandX API credentials (set in `.env`)

### Setup

```bash
# Clone and build
cd standx-rs
cargo build --release

# Create .env file with credentials
cp .env.example .env
# Edit .env with your wallet address and private key

# Run
cargo run --release
```

### Environment Variables

Copy `.env.example` to `.env` and configure:

| Variable | Description |
|----------|-------------|
| `WALLET_AD` | Your wallet address (0x...) |
| `PRIVATE_KEY` | Your private key (without 0x prefix) |

### Configuration

Edit `config.json`:

```json
{
  "symbols": ["BTC-USD"],
  "orderbook_levels": 20,
  "history_minutes": 2,
  "debug": true,
  "strategy": {
    "tick_size": 0.01,
    "vol_to_half_spread": 8.0,
    "skew": 10.0,
    "max_position_dollar": 420.0,
    "c1_ticks": 1600.0,
    "order_qty_dollar": 20.0,
    "lot_size": 0.0001
  },
  "order": {
    "enabled": true,
    "reprice_threshold_bps": 1.0
  },
  "pnl_tracking": {
    "enabled": true,
    "poll_interval_secs": 60,
    "csv_path": "wallet_history.csv"
  },
  "orderbook_sanity_check": {
    "enabled": true,
    "interval_secs": 30,
    "drift_threshold_bps": 1.0
  }
}
```

## Configuration Reference

### General

| Parameter | Description | Default |
|-----------|-------------|---------|
| `symbols` | Trading pairs to quote | `["BTC-USD"]` |
| `orderbook_levels` | Orderbook depth to track | `20` |
| `history_minutes` | Warmup period before trading | `10` |
| `debug` | Enable debug logging | `true` |
| `stats_interval_secs` | Stats logging interval | `30` |

### Strategy (`strategy` section)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `tick_size` | Price tick size | `0.01` |
| `vol_to_half_spread` | Volatility to half-spread multiplier | `8.0` |
| `half_spread_bps` | Fixed half-spread in bps (if vol=0) | `0.0` |
| `skew` | Position skew factor | `10.0` |
| `max_position_dollar` | Max position size in USD | `420.0` |
| `order_qty_dollar` | Order size in USD | `20.0` |
| `c1_ticks` | Alpha (OBI) adjustment in ticks | `1600.0` |
| `looking_depth` | OBI depth as fraction of mid | `0.025` |
| `lot_size` | Minimum order quantity | `0.0001` |
| `step_ns` | Rolling window step size (ns) | `100000000` |
| `window_steps` | Number of steps in rolling window | `6000` |

### Order Management (`order` section)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enabled` | Enable order placement | `false` |
| `reprice_threshold_bps` | Min price change to reprice | `1.0` |
| `pending_timeout_secs` | Order confirmation timeout | `5` |

### PnL Tracking (`pnl_tracking` section)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enabled` | Enable PnL tracking | `false` |
| `poll_interval_secs` | Balance polling interval | `60` |
| `csv_path` | Path to CSV history file | `wallet_history.csv` |

### Orderbook Sanity Check (`orderbook_sanity_check` section)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enabled` | Enable periodic REST API validation | `false` |
| `interval_secs` | Check interval | `30` |
| `drift_threshold_bps` | Drift threshold to trigger correction | `1.0` |

## Project Structure

```
standx-rs/
├── src/
│   ├── main.rs           # Application entry point
│   ├── lib.rs            # Library exports
│   ├── config.rs         # Configuration loading
│   ├── logging.rs        # Logging utilities
│   ├── types.rs          # Core data types
│   ├── orderbook/        # Lock-free orderbook storage
│   │   └── sanity_check.rs  # REST API orderbook validation
│   ├── websocket/        # WebSocket client for market data
│   ├── strategy/         # Quote calculation strategies
│   │   ├── traits.rs     # QuoteStrategy trait
│   │   ├── obi.rs        # OBI strategy implementation
│   │   ├── quotes.rs     # Quote output formatting
│   │   └── rolling.rs    # Rolling statistics
│   └── trading/          # Order execution
│       ├── auth.rs       # JWT authentication
│       ├── client.rs     # REST API client
│       ├── order_ws.rs   # WebSocket order client
│       ├── order_manager.rs  # Order state machine
│       ├── position.rs   # Position polling
│       └── wallet_tracker.rs # PnL tracking with CSV
├── config.json           # Configuration file
├── .env.example          # Environment template
├── .env                  # API credentials (not in git)
├── deploy.py             # Deploy to remote server
├── archive.py            # Create source archive
└── Cargo.toml
```

## Strategy Interface

The codebase uses a trait-based design for easy strategy swapping:

```rust
pub trait QuoteStrategy {
    /// HOT PATH - Process orderbook, return quote
    fn update(&mut self, snapshot: &OrderbookSnapshot) -> Option<Quote>;

    fn set_position(&mut self, position: f64);
    fn is_valid_for_trading(&self) -> bool;
    fn volatility(&self) -> f64;
    fn alpha(&self) -> f64;
    // ...
}
```

### Adding a New Strategy

1. Create `src/strategy/my_strategy.rs`
2. Implement `QuoteStrategy` trait
3. Export from `src/strategy/mod.rs`
4. Change strategy type in `main.rs`

## OBI Strategy Details

The default OBI (Order Book Imbalance) strategy calculates quotes based on:

1. **Volatility**: Rolling std dev of mid-price changes → half-spread
2. **Alpha (OBI)**: Z-score of bid-ask quantity imbalance → fair price adjustment
3. **Position Skew**: Current position → asymmetric spread widening

```
fair_price = mid_price + c1 * alpha
bid_price = fair_price - half_spread * (1 + skew * normalized_position)
ask_price = fair_price + half_spread * (1 - skew * normalized_position)
```

## Latency Budget

| Component | Target |
|-----------|--------|
| `strategy.update()` | <5μs |
| `order_manager.on_quote()` | <10μs |
| `mpsc.try_send()` | <1μs |
| **Total hot path** | **<15μs** |
| WebSocket order send | 5-50ms (async) |

## Safety Features

- **POST-ONLY orders**: All orders are maker-only (no taker fees, no crossing)
- **Position limits**: Stops quoting one side at ±max_position_dollar
- **Graceful shutdown**: Cancels all live orders on Ctrl+C
- **Order timeout**: Clears stuck orders after 5 seconds
- **Auto-reconnect**: WebSocket reconnection with exponential backoff

## Deployment

### Deploy to Remote Server

```bash
# Edit deploy.py to set REMOTE_HOST and KEY_FILE
python deploy.py
```

This uploads source code to the remote server. Build and run manually via SSH.

### Create Source Archive

```bash
python archive.py
```

Creates `standx-rs.zip` excluding sensitive files (.env, .pem) and build artifacts.

## License

MIT
