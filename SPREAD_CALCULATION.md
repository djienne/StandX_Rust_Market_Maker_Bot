# Quote calculation

The current implementation is `src/strategy/obi.rs`; sizing and exposure checks are in `src/trading/equity.rs` and `src/trading/order_manager.rs`.

## Sampling and units

StandX snapshots carry exchange timestamps for ordering and local monotonic receive times for sampling and freshness. At each `step_ns` boundary, statistics use the last observation available at that boundary. A message exactly on the boundary can be used there; a later message cannot fill an earlier sample. Redundant messages between boundaries do not add samples. A gap reaching the depth freshness timeout resets the window and warmup.

For grid interval Δt in seconds and mid-price m in USD per base unit:

```
change[k] = m[k] - m[k-1]
volatility = population_std(change over window_steps) / sqrt(Δt)
```

Volatility has units of price per square-root second. Multiplying it by `vol_to_half_spread` yields a price spread only when that coefficient carries the intended square-root-time horizon. This is a configurable quote heuristic, not a calibrated diffusion forecast. The minimum sample count is 100; live readiness additionally requires `history_minutes` of continuous valid observations.

StandX imbalance is bid quantity minus ask quantity within `looking_depth` of mid. Its rolling z-score uses the same sampling grid. If configured and fresh/warmed up, Binance BTCUSDT imbalance z-score replaces StandX alpha. Binance's separate rolling window remains feed-event based; changing alpha source changes the estimator and must be assessed empirically.

## Prices

Base half-spread uses the first available mode: positive volatility times `vol_to_half_spread`, fixed `half_spread_bps`, fixed `half_spread`, then one tick. `c1` is a coefficient in price units per alpha unit. If it is zero, `c1_ticks` is multiplied by the current exchange tick size.

```
fair_price = mid + c1 * alpha
normalized_position = clamp(position * mid / equity_limit, -1, 1)
level_spread = base_half_spread * level_multiplier
bid_depth = max(0, level_spread * (1 + skew * normalized_position))
ask_depth = max(0, level_spread * (1 - skew * normalized_position))
bid = min(fair_price - bid_depth, best_bid)
ask = max(fair_price + ask_depth, best_ask)
```

Level multipliers are 1 for the inner level and `spread_level_multiplier` for the outer level. Apply the mid-relative minimum half-spread floor, scaled by the level multiplier, then round bids down and asks up to the exchange tick grid. Submitted orders remain post-only.

## Sizing and exposure

For equity E, leverage L, and N levels:

```
per_order_target = max(E * L * 0.18 / N, min_order_qty_dollar)
equity_limit = max(0, (E * L - N * per_order_target) * 0.9)
```

These are sizing assumptions, not a model of every exchange margin rule. The manager checks absolute position plus all other same-side pending/live/canceling orders and the proposed quantity. Outstanding quantities are valued at the greater of their limit price and current mid. Optional absolute/incremental caps and per-order ceilings further restrict headroom; the equity-derived absolute limit always applies.

Permitted quantities round down to the current lot size. Orders below the exchange quantity minimum or configured dollar minimum are skipped. Canceling orders continue to reserve exposure until confirmation or verified reconciliation. Replacements wait for a fresh quote and pass the same checks. Abrupt mark-price movement and delayed exchange position reporting remain limitations of polling-based controls.

## Scientific checks

Validate invariance to redundant message frequency, flat-price zero volatility, known price increments, no use of future observations, and reset across stale gaps. Tests establish those numerical properties; they do not establish positive predictive power or profitable market making. Evaluate fills, adverse selection, fees, funding, and inventory risk separately on representative observations.
