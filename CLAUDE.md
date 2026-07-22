# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Run Commands

**Mandatory release-profile rule:** Always use Cargo's `--release` profile for
builds, runs, checks, lints, and tests in this repository. Do not create or use
debug-profile artifacts; latency and behavior must be evaluated with optimized
code.

```bash
# Build
cargo build --release

# Run with default config
cargo run --release

# Run with custom config
cargo run --release -- /path/to/config.json

# Run tests
cargo test --release

# Run a single test
cargo test --release test_name

# Run tests in a specific module
cargo test --release trading::order_manager::tests

# Check compilation without building
cargo check --release

# Format code
cargo fmt

# Lint
cargo clippy --release
```

## Architecture

### High-Level Data Flow

```
WS Orderbook ─► ObiStrategy.update() ─► OrderManager.on_quote() ─► mpsc ─► OrderExecutor
                    (SYNC, <10μs)           (SYNC, <10μs)                    (async task)
                         │                        │
                         ▼                        ▼
                  SharedPosition            OrderDecision
                  (lock-free read)
```

The system is designed for **sub-15μs hot path latency**. The synchronous hot path (orderbook update → quote generation → order decision) never blocks on async I/O.

### Key Design Patterns

**Lock-Free Data Structures**
- `CurrentOrderbook`: Triple buffer pattern for orderbook state (no mutex on reads)
- `SharedPosition`: Atomic f64 for position reads (lock-free)
- `OrderbookHistory`: Ring buffer for historical snapshots

**Sync/Async Separation**
- Hot path (`ObiStrategy.update()`, `OrderManager.on_quote()`) is synchronous
- Order execution happens via mpsc channel to async task
- Position polling runs in background task, updates atomic

**State Machine for Orders**
```
Pending → Live → Canceling → (cleared)
```
Order manager tracks one bid and one ask order per symbol.

### Module Structure

- `src/strategy/`: Quote generation logic
  - `traits.rs`: `QuoteStrategy` trait (implement this for new strategies)
  - `obi.rs`: OBI (Order Book Imbalance) strategy implementation
  - `rolling.rs`: Rolling statistics (volatility, imbalance z-score)

- `src/orderbook/`: Lock-free orderbook storage
  - `snapshot.rs`: `CurrentOrderbook` triple buffer
  - `history.rs`: `OrderbookHistory` ring buffer
  - `sanity_check.rs`: Periodic REST API validation

- `src/trading/`: Order execution and position management
  - `order_manager.rs`: Synchronous order decision logic (with pending prices, circuit breaker)
  - `order_ws.rs`: WebSocket order client with auto-reconnect (tracks pending cancels)
  - `order_checker.rs`: Background open orders polling and stale detection
  - `position.rs`: Background position polling
  - `auth.rs`: JWT authentication and signing

- `src/websocket/`: Market data WebSocket client
  - `messages.rs`: StandX message parsing
  - `reconnect.rs`: Auto-reconnection with exponential backoff

### Adding a New Strategy

1. Create `src/strategy/my_strategy.rs`
2. Implement the `QuoteStrategy` trait from `src/strategy/traits.rs`
3. Export from `src/strategy/mod.rs`
4. Change strategy type in `main.rs`

The `update()` method is the hot path - keep it under 5μs, avoid allocations.

### Hot Path Optimizations

The following optimizations are applied to minimize hot path latency:

- **Cached statistics** (`rolling.rs`): `RollingStats` caches `std` and `mean` on each `push()` to avoid `sqrt()` on every `zscore()` call
- **Simple loops** (`obi.rs`): `calculate_imbalance()` uses explicit for-loops instead of iterator chains
- **Pre-computed values** (`obi.rs`): `min_depth_tick` calculated once per quote, not twice
- **Pre-allocated vectors**: `Vec::with_capacity()` used where size is known (e.g., `check_timeouts()`)

## Configuration

Main config file: `config.json`
Credentials: `.env` file (WALLET_AD, PRIVATE_KEY)

Key config sections:
- `strategy`: Quote parameters (tick_size, vol_to_half_spread, skew, max_position_dollar)
- `order`: Order management (enabled by default; set `enabled=false` only for explicit test/observe-only mode, plus reprice_threshold_bps, pending_timeout_secs, max_live_age_secs, circuit_breaker_rejections)
- `position`: Position polling (enabled, poll_interval_secs)

## Order Manager Features

**Pending Prices**: When repricing (CancelAndReplace), the intended new price is stored and placed immediately after cancel confirms. This prevents stale prices due to confirmation delay.

**Circuit Breaker**: Automatically pauses trading after N consecutive rejections (configurable via `circuit_breaker_rejections`). Call `reset_circuit_breaker()` to resume.

**Open Orders Checker**: Background task polls exchange for open orders every 3s. Signals stale state when:
- Exchange reports 0 orders but we think we have some
- Orders are imbalanced (all on same side)
- Orders exceed `max_order_age_secs` (2x max_live_age by default)

## Clock Sources

The codebase uses StandX server time (`received_at` from orderbook messages) for order timeout checking to avoid clock drift issues. Local system time is only used for:
- Session prefix uniqueness (order IDs)
- Position staleness checks (acceptable tolerance)

## Docker Deployment

### Quick Start

```bash
# 1. Setup credentials
cp .env.example .env
# Edit .env with WALLET_AD and PRIVATE_KEY

# 2. Test connection (no real orders)
docker compose --profile test up bot-test

# 3. Production (real order placement is enabled by default)
docker compose up -d
docker compose logs -f bot
```

### Commands

```bash
docker compose up -d              # Start in background
docker compose down               # Stop (cancels all orders)
docker compose logs -f bot        # Follow logs
docker compose restart bot        # Restart after config change
docker compose --profile test up bot-test   # Test mode
```

### Files

- `Dockerfile`: Multi-stage build (Rust 1.85, ~50MB runtime image)
- `docker-compose.yml`: Production (`bot`) and test (`bot-test`) services
- `config.test.json`: Test config with `order.enabled=false`
- `data/`: PnL history CSV (persisted across restarts)
