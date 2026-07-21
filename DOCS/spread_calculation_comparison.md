# Spread Calculation Comparison: Python Backtest vs Rust Production

## Overview

This document compares the spread calculation logic in `backtest_obi.py` (Python) with `src/strategy/obi.rs` (Rust) to identify any inconsistencies.

**Scope**: Only first bid/ask level (level 0). Rust supports 2 levels but we're comparing the level 0 logic only.

---

## Summary

| Component | Status | Notes |
|-----------|--------|-------|
| Half-spread calculation | ✅ Consistent | vol > bps > fixed priority |
| Skew/depth formula | ✅ Consistent | `bid_depth = spread × (1 + skew × pos)` |
| BBO clamping | ✅ Consistent | Never cross the spread |
| Tick rounding | ✅ Consistent | Floor bids, ceil asks |
| **min_half_spread_bps floor** | ❌ **Inconsistent** | Different application point |

---

## Detailed Comparison

### 1. Half-Spread Calculation ✅ CONSISTENT

**Rust** (`src/strategy/obi.rs:345-357`):
```rust
let base_half_spread_tick = if vol_to_half_spread > 0.0 && volatility > 0.0 {
    (volatility * vol_to_half_spread) / tick_size
} else if half_spread_bps > 0.0 {
    mid_price * (half_spread_bps / 10000.0) / tick_size
} else if half_spread > 0.0 {
    half_spread / tick_size
} else {
    1.0  // fallback
};
```

**Python** (`backtest_obi.py` lines ~247-262):
```python
if vol_to_half_spread > 0 and np.isfinite(volatility):
    half_spread_price = volatility * vol_to_half_spread
    half_spread_tick = half_spread_price / tick_size
elif half_spread_bps > 0:
    half_spread_tick = mid_price * (half_spread_bps / 10000.0) / tick_size
elif half_spread > 0:
    half_spread_tick = half_spread / tick_size
```

**Verdict**: Identical priority order (volatility > bps > fixed) and formulas.

---

### 2. Skew/Depth Calculation ✅ CONSISTENT

**Rust** (`obi.rs:371-407`):
```rust
// Position normalization
let normalized_position = (position * mid_price) / max_position_dollar;
let clamped_position = normalized_position.clamp(-1.0, 1.0);

// Depth calculation with skew
let bid_depth_tick = (half_spread_tick * (1.0 + skew * clamped_position)).max(0.0);
let ask_depth_tick = (half_spread_tick * (1.0 - skew * clamped_position)).max(0.0);
```

**Python** (lines ~274-289):
```python
# Position normalization
notional_position = position * mid_price
normalized_position = notional_position / max_position_dollar
clamped_position = max(-1.0, min(1.0, normalized_position))

# Depth calculation with skew
bid_depth_tick = half_spread_tick * (1.0 + skew * clamped_position)
ask_depth_tick = half_spread_tick * (1.0 - skew * clamped_position)
if bid_depth_tick < 0:
    bid_depth_tick = 0.0
if ask_depth_tick < 0:
    ask_depth_tick = 0.0
```

**Verdict**: Identical formulas. Both clamp position to [-1, 1] and apply symmetric skew.

---

### 3. Bid/Ask Price from Fair Price ✅ CONSISTENT

**Rust** (`obi.rs:410-415`):
```rust
// Calculate raw quote prices
let raw_bid = fair_price - bid_depth_tick * tick_size;
let raw_ask = fair_price + ask_depth_tick * tick_size;

// Clamp to BBO (never cross the spread)
let clamped_bid = raw_bid.min(best_bid);
let clamped_ask = raw_ask.max(best_ask);
```

**Python** (lines ~291-299):
```python
bid_price = min(
    fair_price - bid_depth_tick * tick_size,
    best_bid,
)
ask_price = max(
    fair_price + ask_depth_tick * tick_size,
    best_ask,
)
```

**Verdict**: Identical. Both ensure quotes don't cross the BBO.

---

### 4. Tick Rounding ✅ CONSISTENT

**Rust** (`obi.rs:444-446`):
```rust
bid_prices[level] = (floored_bid / tick_size).floor() * tick_size;
ask_prices[level] = (floored_ask / tick_size).ceil() * tick_size;
```

**Python** (lines ~300-302):
```python
bid_price = np.floor(bid_price / tick_size) * tick_size
ask_price = np.ceil(ask_price / tick_size) * tick_size
```

**Verdict**: Identical. Floor for bids (round down), ceil for asks (round up).

---

### 5. Minimum Half-Spread Floor ❌ INCONSISTENT

This is the **key difference** between the implementations.

#### Rust Implementation (`obi.rs:417-442`)

Applies floor to **final prices AFTER BBO clamping**:

```rust
// Apply floor AFTER BBO clamping
// Floor ensures minimum distance from mid_price
let (floored_bid, bid_floor_applied) = if min_half_spread_bps > 0.0 {
    let min_bid = mid_price * (1.0 - min_half_spread_bps / 10000.0);
    if clamped_bid > min_bid {  // bid is too high (tight)
        (min_bid, true)
    } else {
        (clamped_bid, false)
    }
} else {
    (clamped_bid, false)
};

let (floored_ask, ask_floor_applied) = if min_half_spread_bps > 0.0 {
    let min_ask = mid_price * (1.0 + min_half_spread_bps / 10000.0);
    if clamped_ask < min_ask {  // ask is too low (tight)
        (min_ask, true)
    } else {
        (clamped_ask, false)
    }
} else {
    (clamped_ask, false)
};
```

#### Python Implementation (lines ~263-267)

Applies floor to **half_spread_tick BEFORE skew calculation**:

```python
# Enforce minimum half-spread floor
if min_half_spread_bps > 0:
    min_half_spread_tick = (mid_price * (min_half_spread_bps / 10000.0)) / tick_size
    half_spread_tick = max(half_spread_tick, min_half_spread_tick)
```

#### Comparison Table

| Aspect | Python | Rust |
|--------|--------|------|
| **When applied** | BEFORE skew calculation | AFTER BBO clamping |
| **What gets floored** | `half_spread_tick` itself | Final bid/ask prices |
| **Floor reference** | Indirectly via half_spread_tick | Directly from mid_price |
| **Effect on skew** | Skew applied to floored value | Floor applied symmetrically |

#### Example Showing the Difference

Given:
- `mid_price = 100`
- `min_half_spread_bps = 5` (5 bps = 0.05%)
- `half_spread_tick = 2` ticks (= 0.02 with tick_size=0.01)
- Position is long: `clamped_position = 0.5`, `skew = 1.0`
- `fair_price = mid_price` (assume alpha = 0)

**Python calculation**:
1. `min_half_spread_tick = 100 * 0.0005 / 0.01 = 5` ticks
2. `half_spread_tick = max(2, 5) = 5` ticks ← **floored here**
3. `bid_depth_tick = 5 * (1 + 1.0 * 0.5) = 7.5` ticks
4. `ask_depth_tick = 5 * (1 - 1.0 * 0.5) = 2.5` ticks
5. `bid_price = 100 - 7.5 * 0.01 = 99.925`
6. `ask_price = 100 + 2.5 * 0.01 = 100.025`
7. **Result**: Bid is 7.5 ticks wide, ask is 2.5 ticks wide (asymmetric)

**Rust calculation**:
1. `half_spread_tick = 2` ticks (no floor yet)
2. `bid_depth_tick = 2 * (1 + 1.0 * 0.5) = 3` ticks
3. `ask_depth_tick = 2 * (1 - 1.0 * 0.5) = 1` tick
4. `raw_bid = 100 - 3 * 0.01 = 99.97`
5. `raw_ask = 100 + 1 * 0.01 = 100.01`
6. Clamp to BBO (assume no change)
7. Apply floor: `min_bid = 99.95`, `min_ask = 100.05`
8. `floored_bid = min(99.97, 99.95) = 99.95` ← **floored here**
9. `floored_ask = max(100.01, 100.05) = 100.05` ← **floored here**
10. **Result**: Both bid and ask are exactly 5 bps from mid_price (symmetric)

---

## Recommendation

To make Python backtest consistent with Rust production, modify the floor logic in `backtest_obi.py`:

### Current Python Code (INCONSISTENT)

```python
# Lines ~263-267 - Floor applied BEFORE skew
if min_half_spread_bps > 0:
    min_half_spread_tick = (mid_price * (min_half_spread_bps / 10000.0)) / tick_size
    half_spread_tick = max(half_spread_tick, min_half_spread_tick)

# ... skew calculation uses floored half_spread_tick ...

bid_price = min(fair_price - bid_depth_tick * tick_size, best_bid)
ask_price = max(fair_price + ask_depth_tick * tick_size, best_ask)

bid_price = np.floor(bid_price / tick_size) * tick_size
ask_price = np.ceil(ask_price / tick_size) * tick_size
```

### Proposed Python Code (MATCHES RUST)

```python
# REMOVE the early floor (lines ~263-267)
# The half_spread_tick should NOT be floored before skew

# ... skew calculation uses original half_spread_tick ...

bid_price = min(fair_price - bid_depth_tick * tick_size, best_bid)
ask_price = max(fair_price + ask_depth_tick * tick_size, best_ask)

# ADD: Apply min floor AFTER BBO clamping (matching Rust obi.rs:417-442)
if min_half_spread_bps > 0:
    min_bid = mid_price * (1.0 - min_half_spread_bps / 10000.0)
    min_ask = mid_price * (1.0 + min_half_spread_bps / 10000.0)
    if bid_price > min_bid:
        bid_price = min_bid
    if ask_price < min_ask:
        ask_price = min_ask

bid_price = np.floor(bid_price / tick_size) * tick_size
ask_price = np.ceil(ask_price / tick_size) * tick_size
```

---

## Files Referenced

- **Rust**: `src/strategy/obi.rs` (lines 336-497)
- **Python**: `backtest_obi.py` (function `obi_mm`, lines ~40-340)

---

## Impact Assessment

The difference mainly affects scenarios where:
1. `min_half_spread_bps` is active (non-zero)
2. The calculated volatility-based spread is below the minimum
3. There is non-zero position (skew is active)

In these cases, Python will have asymmetric spreads (skew applied to the floored value), while Rust will have symmetric minimum spreads (floor applied to final prices).

For backtesting accuracy, the Python should match Rust behavior to ensure simulation fidelity.
