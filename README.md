# StandX Market Maker

Rust market maker for StandX BTC-USD perpetual futures. Uses StandX depth for prices and volatility, Binance BTCUSDT order-book imbalance for alpha (with a StandX fallback), and account equity for sizing.

[Associated video](https://youtu.be/7P3MwTRjy2I) · [Project referral link](https://standx.com/referral?code=FREQTRADEFR)

## Run

Use Rust 1.92 or newer. All compilation and validation use the release profile.

```bash
cp .env.example .env
# Configure WALLET_AD and PRIVATE_KEY locally.
cargo build --release --locked --bin standx-orderbook
./target/release/standx-orderbook config.test.json
```

`config.test.json` is observe-only. It polls account data when credentials are supplied but does not submit orders or change leverage. `config.json` enables real orders. Live startup authenticates, verifies/sets the configured exchange leverage, and cancels existing account orders before trading. Use a dedicated account: reconciliation and shutdown cancel orders account-wide.

Only a single BTC-USD symbol is supported for live trading. Multi-symbol observation remains available; Binance alpha is BTC-specific and is not a general cross-asset model. Keep leverage at 1 unless you have separately assessed the resulting exposure.

## Docker

```bash
docker compose build
docker compose --profile test up bot-test
# Live trading, when intentionally enabled:
docker compose up -d bot
docker compose logs -f bot
docker compose stop bot
```

The image contains the Rust bot and runtime configuration, with credentials supplied through `.env` at runtime. Build context excludes credentials and generated data. Wallet CSVs persist under `data/` (`data/observe/` for the test service). Logs rotate at 50 MB with five files. Services have a 120-second stop grace period and do not automatically restart after an unverified cleanup.

## Behavior and configuration

The checked-in JSON files are runnable configurations; `src/config.rs` defines omitted-field defaults and validation. Important controls:

| Setting | Meaning |
|---|---|
| `order.enabled` | Real orders; true by default, false in the test configuration |
| `history_minutes` | Continuous valid-data warmup; 10 minutes in both configurations |
| `strategy.step_ns` | Fixed sampling interval; 100 ms in both configurations |
| `strategy.window_steps` | Number of grid samples in rolling statistics; 6000 = 10 minutes at 100 ms |
| `strategy.update_interval_steps` | Minimum grid steps between quote updates |
| `websocket.depth_stale_timeout_secs` | Maximum age of valid depth; defaults to 5 seconds |
| `websocket.stale_timeout_secs` | Separate transport inactivity timeout; defaults to 60 seconds |
| `position.stale_threshold_secs` | Maximum position age; defaults to 10 seconds |
| `pnl_tracking.stale_threshold_secs` | Maximum equity age; defaults to 120 seconds |
| `strategy.max_order_qty_dollar` | Optional additional per-order notional ceiling |
| `order.absolute_max_position_dollar` | Optional additional exposure ceiling |
| `order.position_limit_from_start` | Apply the optional ceiling relative to the initial position; the absolute equity-derived limit still applies |
| `orderbook_sanity_check` | REST/WS diagnostic comparison; 30-second interval in both configurations |

Tick size, lot size, and minimum order quantity come from the exchange at startup. Invalid or unavailable symbol metadata stops startup. Metadata changes pause trading, invalidate queued submissions, reconcile orders, and restart warmup before using the new precision.

See [spread calculation](SPREAD_CALCULATION.md) for the estimator, units, quote formulas, and sizing assumptions. Parameters have not been calibrated for profitability by the correctness tests.

## Execution safety

- New orders are post-only. Pending, live, and canceling orders all reserve exposure; quantities round down to the lot grid. Additional caps can only reduce the permitted exposure.
- Pausing invalidates queued decisions. A submission barrier waits for an admitted write before cleanup. Failed writes remain unresolved because the exchange may have received them.
- REST order lookup and correlated responses resolve submissions. An empty open-order snapshot alone cannot resolve an unknown submission. Reconciliation stays paused until submissions are settled and cancellation is verified; unresolved cases require inspection rather than blind restart.
- Fills pause new orders until subsequent position publications. Reconciliation also requires two later position publications before resuming. This is a conservative polling gate, not a guarantee of exchange consistency or protection against abrupt price gaps.
- Valid depth freshness is independent of ping/pong traffic. Invalid, duplicate, regressing, or old queued snapshots do not extend it. Stale depth triggers cancellation, reconnection, and a new warmup.
- Circuit-breaker and safety pauses recover only through a verified reconciliation, requested after their configured cooldown.
- Ctrl+C, SIGTERM, and fatal trading-task exits share cleanup. Failed cleanup exits with an error. Positions are reported and left open; stopping does not flatten them.
- REST and WebSocket books are not simultaneous observations. Their diagnostic difference is not treated as ground truth and never overwrites the trading book.

## Development and validation

```bash
cargo test --release --lib --bin standx-orderbook --locked
cargo check --release --all-targets --locked
cargo bench --profile release --bench hot_path --locked
```

Tests cover order accounting, delayed responses and submission barriers, valid-depth outages, and fixed-grid sampling. Benchmarks measure selected local operations, not exchange execution latency. See [monitoring](MONITORING_RUNBOOK.md) for run checks and shutdown verification.

Depth messages are parsed once, on the market-data reader task, straight into a fixed-size validated snapshot; the main loop only accepts, quotes, decides, and hands decisions to the executor. Order writes sign with a copy of the keypair, so they never wait on the auth mutex that REST polls hold across round trips. The quote/decision path is synchronous. Current-book diagnostic storage uses a small `RwLock`; there is no full-snapshot history recorder or unused Binance BBO connection. The unrelated Lighter collector has been removed; existing data files are untouched.

## License

MIT
