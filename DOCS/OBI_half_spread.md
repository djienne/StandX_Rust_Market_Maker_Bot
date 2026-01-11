# OBI half-spread and quote calculation

This document explains how the Order Book Imbalance (OBI) backtest computes the half-spread
and turns it into bid/ask quotes. The logic is in `backtest_lighter_OBI.py` (function `obi_mm`).
Everything below is written so you can re-implement it in Rust or any language.

## Key inputs

- `tick_size`: price tick size from the market data.
- `step_ns`: simulation step in nanoseconds.
- `window_steps`: length of the rolling window (number of steps) used for alpha and volatility.
- `update_interval_steps`: how often (in steps) alpha/volatility are updated.
- `vol_to_half_spread`: multiplier that converts volatility to half-spread (in ticks). Default: `--vol-to-half-spread=8.0`.
- `half_spread`: fixed half-spread in price units (optional).
- `half_spread_bps`: fixed half-spread in basis points (optional).
- `min_grid_step`: minimum grid price step (in price units).
- `skew`: position-based skew factor. Default: `--skew=1.0`.
- `max_position_dollar`: position cap used for normalization. Default: `--max-position-dollar=500.0` (or `order_qty_dollar * max_position_multiplier` if <= 0).
- `c1`: coefficient for alpha adjustment in fair price. Default: `--c1-ticks=160` (so `c1=160 * tick_size`).
- `looking_depth`: percentage depth (e.g., 0.05 for 5%) used to sum order book quantities for imbalance. Default: `--looking-depth=0.025`.
- `grid_num`: number of grid levels to quote on each side.
- `order_qty_dollar`: standard order size in dollar value. Default: `--order-qty-dollar=20.0`.

## Defaults (backtest_lighter_OBI.py)

These are the CLI defaults used by `backtest_lighter_OBI.py` when you do not override them.

- `data_dir="lighter_data"`
- `out="lighter_data/crv_hft_obi.npz"`
- `max_rows=None`
- `latency_ns=1_000_000`
- `record_every=10`
- `step_ns=100_000_000`
- `window_steps=6000`
- `update_interval_steps=50`
- `order_qty_dollar=20.0`
- `max_position_dollar=500.0` (if None or <= 0, uses `order_qty_dollar * max_position_multiplier`, default `max_position_multiplier=50.0`)
- `grid_num=1`
- `vol_to_half_spread=8.0`
- `half_spread=None`
- `half_spread_bps=0.0`
- `half_spread_ticks=None`
- `skew=1.0`
- `skew_ticks=None`
- `c1=None` (falls back to `c1_ticks=160`, so `c1=160 * tick_size`)
- `grid_interval=None` (falls back to `grid_interval_ticks=1`, so `min_grid_step=tick_size`)
- `looking_depth=0.025`
- `roi_lb=None`, `roi_ub=None` (inferred from data with `roi_pad=0.02`)
- `plots_dir="plots"`
- `run_backtest=True`

## Definitions

- `mid_price = (best_bid + best_ask) / 2`
- `mid_price_tick = mid_price / tick_size`
- `mid_price_chg[t] = mid_price_tick - prev_mid_price_tick`
- `vol_scale = sqrt(1_000_000_000 / step_ns)` (converts per-step std to per-second scale)
- `volatility = std(mid_price_chg over window) * vol_scale`
- `alpha = zscore(imbalance over window)` (see OBI section below)

## Imbalance calculation

The `imbalance` used for the alpha signal is the volume imbalance within a specific depth relative to the `mid_price`, while respecting the ROI (Region of Interest) bounds.

```python
# Sum quantities up to looking_depth relative to mid_price
# roi_lb_tick and roi_ub_tick are bounds for the pre-allocated depth arrays

sum_ask_qty = 0.0
from_tick = max(depth.best_ask_tick, roi_lb_tick)
upto_tick = min(
    int(np.floor(mid_price * (1.0 + looking_depth) / tick_size)),
    roi_ub_tick,
)
for price_tick in range(from_tick, upto_tick):
    sum_ask_qty += depth.ask_depth[price_tick - roi_lb_tick]

sum_bid_qty = 0.0
from_tick = min(depth.best_bid_tick, roi_ub_tick)
upto_tick = max(
    int(np.ceil(mid_price * (1.0 - looking_depth) / tick_size)),
    roi_lb_tick,
)
for price_tick in range(from_tick, upto_tick, -1):
    sum_bid_qty += depth.bid_depth[price_tick - roi_lb_tick]

imbalance = sum_bid_qty - sum_ask_qty
```

## Step-by-step logic

1. Each simulation step advances by `step_ns`.
2. If `t % update_interval_steps == 0` and `t >= window_steps - 1`:
   - Update `alpha` (z-score of imbalance).
   - Update `volatility` from recent mid-price changes.
3. Compute half-spread (in ticks) using **one of three modes**:
   - Volatility mode (preferred when `vol_to_half_spread > 0` and volatility is finite).
   - BPS mode (if `half_spread_bps > 0`).
   - Fixed price mode (if `half_spread > 0`).
4. If half-spread is non-finite or <= 0, the step is skipped.
5. Convert half-spread into bid/ask depths (with skew), then quote prices.
6. Snap prices to a grid defined by `min_grid_step` and the half-spread.

## Half-spread calculation

This is directly from the Python code:

```python
# last_half_spread_tick is carried across steps
half_spread_tick = last_half_spread_tick

if vol_to_half_spread > 0 and np.isfinite(volatility):
    # volatility mode
    half_spread_tick = volatility * vol_to_half_spread
elif half_spread_bps > 0:
    # bps mode: convert bps to ticks using current mid_price
    half_spread_tick = mid_price * (half_spread_bps / 10000.0) / tick_size
elif half_spread > 0:
    # fixed price mode: convert price to ticks
    half_spread_tick = half_spread / tick_size

last_half_spread_tick = half_spread_tick
```

Notes:
- If `vol_to_half_spread` is positive, it overrides the bps and fixed modes
  **only when volatility is finite**.
- If volatility is not available yet, the previous half-spread is reused.
- If the result is not finite or <= 0, the step is skipped.

## Volatility estimation

```python
vol_scale = np.sqrt(1_000_000_000 / step_ns)

# every update interval, after enough history
if t >= window_steps - 1:
    window_start = t + 1 - window_steps
    volatility = np.nanstd(mid_price_chg[window_start : t + 1]) * vol_scale
else:
    volatility = np.nan
```

The mid-price changes are computed in **ticks**, so the volatility is also in ticks.
This is why half-spread is in ticks too.

## Quote price calculation (context)

Once half-spread is known, the OBI strategy computes fair price and skewed depths:

```python
# fair price adjustment using alpha
fair_price = mid_price + c1 * alpha

normalized_position = (position * mid_price) / max_position_dollar

# skew adjusts depth based on position
bid_depth_tick = half_spread_tick * (1.0 + skew * normalized_position)
ask_depth_tick = half_spread_tick * (1.0 - skew * normalized_position)
bid_depth_tick = max(bid_depth_tick, 0.0)
ask_depth_tick = max(ask_depth_tick, 0.0)

bid_price = min(fair_price - bid_depth_tick * tick_size, best_bid)
ask_price = max(fair_price + ask_depth_tick * tick_size, best_ask)
```

## Effective Spreads from Mid-Price

The final limit orders are placed at asymmetric distances from the current `mid_price` due to the alpha and position skew.

- **Lower Spread** (Distance to Best Bid Order):
  `Dist_Bid = (Skewed_Bid_Depth) - (Alpha_Adjustment)`
  *(If Alpha is positive/bullish, the bid moves closer to the mid-price, decreasing the spread)*

- **Upper Spread** (Distance to Best Ask Order):
  `Dist_Ask = (Skewed_Ask_Depth) + (Alpha_Adjustment)`
  *(If Alpha is positive/bullish, the ask moves further from the mid-price, increasing the spread)*

Where:
- `Alpha_Adjustment = c1 * alpha`
- `Skewed_Bid_Depth = half_spread * (1 + skew * position_ratio)`
- `Skewed_Ask_Depth = half_spread * (1 - skew * position_ratio)`

Then prices are snapped to the tick grid and a dynamic grid interval:

```python
# snap to tick size
bid_price = np.floor(bid_price / tick_size) * tick_size
ask_price = np.ceil(ask_price / tick_size) * tick_size

# grid interval derived from half-spread and min_grid_step
grid_interval = max(
    np.round(half_spread_tick * tick_size / min_grid_step) * min_grid_step,
    min_grid_step,
)

bid_price = np.floor(bid_price / grid_interval) * grid_interval
ask_price = np.ceil(ask_price / grid_interval) * grid_interval
```

## Warm-up time

The strategy requires a "warm-up" period to fill the rolling window before it can calculate the first `alpha` and `volatility`.

- **Calculation Trigger**: The first update occurs at step `t` when `t >= window_steps - 1` AND `t % update_interval_steps == 0`.
- **First Step**: `t_start = ceil((window_steps - 1) / update_interval_steps) * update_interval_steps`
- **Elapsed Time**: `t_start * step_ns`

**Example (Default Params):**
- `window_steps = 6000`
- `update_interval_steps = 50`
- `step_ns = 100,000,000` (100ms)
- `t_start = 6000`
- **Warm-up Time**: `6000 * 100ms = 600 seconds (10 minutes)`

During this warm-up period, `volatility` is `NaN`, and the strategy will remain inactive (or use fallback modes if configured) until the first valid calculation.

## Summary of half-spread modes

- **Volatility mode**: `half_spread_tick = volatility * vol_to_half_spread`
- **BPS mode**: `half_spread_tick = (mid_price * bps / 10000) / tick_size`
- **Fixed price mode**: `half_spread_tick = half_spread / tick_size`

Only one mode is active at a time, chosen in the order above.

## Order management

### Order Quantity
The strategy converts a fixed dollar amount into a lot-sized quantity.

```python
# order_qty_dollar is the target value
order_qty = max(
    round((order_qty_dollar / mid_price) / lot_size) * lot_size,
    lot_size
)
```

### Multi-level Grid
If `grid_num > 1`, the strategy places multiple orders on each side, spaced by `grid_interval`.

```python
# Bid side
current_bid = bid_price
for _ in range(grid_num):
    place_limit_order(side=BUY, price=current_bid, qty=order_qty)
    current_bid -= grid_interval

# Ask side
current_ask = ask_price
for _ in range(grid_num):
    place_limit_order(side=SELL, price=current_ask, qty=order_qty)
    current_ask += grid_interval
```

Note: Position checks (`normalized_position`) are applied before placing orders to prevent over-accumulation:
- Stop buying if `normalized_position >= 1.0` (max long reached).
- Stop selling if `normalized_position <= -1.0` (max short reached).
