from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np

from convert_standx import convert_parquet_to_npz
from backtest_utils import (
    load_meta,
    print_meta_summary,
    save_plots,
    extract_backtest_results,
    print_backtest_summary,
    load_fees_from_config,
    build_gap_context,
    load_symbol_from_config,
)
from backtest_common import (
    njit,
    NumbaDict as Dict,
    uint64,
    float64,
    BUY,
    SELL,
    GTX,
    LIMIT,
    init_hft_constants,
    infer_roi_bounds,
    BacktestAPI,
    build_asset,
    compute_backtest_params,
    add_common_args,
    add_backtest_args,
    add_run_backtest_args,
)


@njit
def obi_mm(
    hbt,
    recorder,
    record_every,
    step_ns,
    max_steps,
    vol_to_half_spread,
    min_grid_step,
    half_spread,
    half_spread_bps,
    skew,
    c1,
    looking_depth,
    window_steps,
    update_interval_steps,
    order_qty_dollar,
    max_position_dollar,
    grid_num,
    roi_lb,
    roi_ub,
    base_ts_ns,
    gap_starts_ns,
    gap_ends_ns,
    min_half_spread_bps,
):
    asset_no = 0
    imbalance_timeseries = np.full(max_steps, np.nan, np.float64)
    mid_price_chg = np.full(max_steps, np.nan, np.float64)
    half_spread_bps_arr = np.full(max_steps, np.nan, np.float64)

    tick_size = hbt.depth(asset_no).tick_size
    lot_size = hbt.depth(asset_no).lot_size

    t = 0
    record_counter = 0
    last_alpha = 0.0
    prev_mid_price = np.nan
    volatility = np.nan
    last_half_spread_tick = np.nan
    vol_scale = np.sqrt(1_000_000_000 / step_ns)
    roi_lb_tick = int(round(roi_lb / tick_size))
    roi_ub_tick = int(round(roi_ub / tick_size))
    gap_idx = 0
    gap_count = len(gap_starts_ns)
    in_gap = False
    if roi_lb_tick > roi_ub_tick:
        tmp = roi_lb_tick
        roi_lb_tick = roi_ub_tick
        roi_ub_tick = tmp

    # Rolling statistics accumulators (Welford's algorithm)
    imb_sum = 0.0
    imb_sum_sq = 0.0
    imb_count = 0
    chg_sum = 0.0
    chg_sum_sq = 0.0
    chg_count = 0

    # Pre-allocate dictionaries for order management (reused each iteration)
    new_bid_orders = Dict.empty(uint64, float64)
    new_ask_orders = Dict.empty(uint64, float64)

    while hbt.elapse(step_ns) == 0:
        curr_ts_ns = base_ts_ns + (t + 1) * step_ns
        while gap_idx < gap_count and curr_ts_ns >= gap_ends_ns[gap_idx]:
            gap_idx += 1
        gap_active = gap_idx < gap_count and curr_ts_ns >= gap_starts_ns[gap_idx]
        if gap_active:
            hbt.clear_last_trades(asset_no)
            hbt.clear_inactive_orders(asset_no)
            orders = hbt.orders(asset_no)
            if not in_gap:
                order_values = orders.values()
                while order_values.has_next():
                    order = order_values.get()
                    if order.cancellable:
                        hbt.cancel(asset_no, order.order_id, False)
            if record_every > 0 and record_counter % record_every == 0:
                recorder.record(hbt)
            record_counter += 1
            in_gap = True
            t += 1
            if t >= max_steps:
                break
            continue
        in_gap = False

        hbt.clear_inactive_orders(asset_no)

        depth = hbt.depth(asset_no)
        position = hbt.position(asset_no)
        orders = hbt.orders(asset_no)

        if record_every > 0 and record_counter % record_every == 0:
            recorder.record(hbt)
        record_counter += 1

        best_bid = depth.best_bid
        best_ask = depth.best_ask
        if not np.isfinite(best_bid) or not np.isfinite(best_ask):
            t += 1
            if t >= max_steps:
                break
            continue

        mid_price = (best_bid + best_ask) / 2.0
        if np.isfinite(prev_mid_price):
            mid_price_chg[t] = mid_price - prev_mid_price  # Dollar changes
        prev_mid_price = mid_price

        # Vectorized ask depth summation
        ask_from_tick = max(depth.best_ask_tick, roi_lb_tick)
        ask_upto_tick = min(
            int(np.floor(mid_price * (1.0 + looking_depth) / tick_size)),
            roi_ub_tick,
        )
        ask_from_idx = ask_from_tick - roi_lb_tick
        ask_to_idx = ask_upto_tick - roi_lb_tick
        if ask_to_idx > ask_from_idx:
            sum_ask_qty = np.sum(depth.ask_depth[ask_from_idx:ask_to_idx])
        else:
            sum_ask_qty = 0.0

        # Vectorized bid depth summation
        bid_from_tick = min(depth.best_bid_tick, roi_ub_tick)
        bid_upto_tick = max(
            int(np.ceil(mid_price * (1.0 - looking_depth) / tick_size)),
            roi_lb_tick,
        )
        bid_from_idx = bid_upto_tick - roi_lb_tick + 1
        bid_to_idx = bid_from_tick - roi_lb_tick + 1
        if bid_to_idx > bid_from_idx:
            sum_bid_qty = np.sum(depth.bid_depth[bid_from_idx:bid_to_idx])
        else:
            sum_bid_qty = 0.0

        current_imbalance = sum_bid_qty - sum_ask_qty
        imbalance_timeseries[t] = current_imbalance

        # Update rolling imbalance sums - add new value
        if np.isfinite(current_imbalance):
            imb_sum += current_imbalance
            imb_sum_sq += current_imbalance * current_imbalance
            imb_count += 1

        # Remove old value when window is full
        if t >= window_steps:
            old_imb = imbalance_timeseries[t - window_steps]
            if np.isfinite(old_imb):
                imb_sum -= old_imb
                imb_sum_sq -= old_imb * old_imb
                imb_count -= 1

        # Update rolling mid_price_chg sums
        current_chg = mid_price_chg[t]
        if np.isfinite(current_chg):
            chg_sum += current_chg
            chg_sum_sq += current_chg * current_chg
            chg_count += 1

        if t >= window_steps:
            old_chg = mid_price_chg[t - window_steps]
            if np.isfinite(old_chg):
                chg_sum -= old_chg
                chg_sum_sq -= old_chg * old_chg
                chg_count -= 1

        # Compute stats using O(1) running sums instead of O(window_steps)
        if update_interval_steps > 0 and t % update_interval_steps == 0:
            if imb_count >= 2:
                imb_mean = imb_sum / imb_count
                imb_var = (imb_sum_sq / imb_count) - (imb_mean * imb_mean)
                imb_std = np.sqrt(max(0.0, imb_var))
                if imb_std > 0:
                    last_alpha = (current_imbalance - imb_mean) / imb_std
                else:
                    last_alpha = 0.0
            else:
                last_alpha = 0.0

            if chg_count >= 2:
                chg_mean = chg_sum / chg_count
                chg_var = (chg_sum_sq / chg_count) - (chg_mean * chg_mean)
                volatility = np.sqrt(max(0.0, chg_var)) * vol_scale
            else:
                volatility = np.nan

        order_qty = max(
            round((order_qty_dollar / mid_price) / lot_size) * lot_size,
            lot_size,
        )
        fair_price = mid_price + c1 * last_alpha

        notional_position = position * mid_price
        normalized_position = notional_position / max_position_dollar

        half_spread_tick = last_half_spread_tick
        if vol_to_half_spread > 0 and np.isfinite(volatility):
            half_spread_price = volatility * vol_to_half_spread  # in dollars
            half_spread_tick = half_spread_price / tick_size  # convert to ticks
        elif half_spread_bps > 0:
            half_spread_tick = (
                mid_price * (half_spread_bps / 10000.0) / tick_size
            )
        elif half_spread > 0:
            half_spread_tick = half_spread / tick_size

        last_half_spread_tick = half_spread_tick
        half_spread_bps_arr[t] = (half_spread_tick * tick_size / mid_price) * 10000.0

        if not np.isfinite(half_spread_tick) or half_spread_tick <= 0:
            t += 1
            if t >= max_steps:
                break
            continue

        # Clamp normalized position to [-1, 1] for skew formula
        clamped_position = max(-1.0, min(1.0, normalized_position))
        bid_depth_tick = half_spread_tick * (1.0 + skew * clamped_position)
        ask_depth_tick = half_spread_tick * (1.0 - skew * clamped_position)
        if bid_depth_tick < 0:
            bid_depth_tick = 0.0
        if ask_depth_tick < 0:
            ask_depth_tick = 0.0

        bid_price = min(
            fair_price - bid_depth_tick * tick_size,
            best_bid,
        )
        ask_price = max(
            fair_price + ask_depth_tick * tick_size,
            best_ask,
        )

        # Apply min_half_spread_bps floor AFTER BBO clamping (matching Rust obi.rs:417-442)
        if min_half_spread_bps > 0:
            min_bid = mid_price * (1.0 - min_half_spread_bps / 10000.0)
            min_ask = mid_price * (1.0 + min_half_spread_bps / 10000.0)
            if bid_price > min_bid:
                bid_price = min_bid
            if ask_price < min_ask:
                ask_price = min_ask

        bid_price = np.floor(bid_price / tick_size) * tick_size
        ask_price = np.ceil(ask_price / tick_size) * tick_size
        grid_interval = max(
            np.round(half_spread_tick * tick_size / min_grid_step) * min_grid_step,
            min_grid_step,
        )
        if not np.isfinite(grid_interval) or grid_interval <= 0:
            t += 1
            if t >= max_steps:
                break
            continue

        bid_price = np.floor(bid_price / grid_interval) * grid_interval
        ask_price = np.ceil(ask_price / grid_interval) * grid_interval

        # Clear and reuse pre-allocated dictionaries
        for k in list(new_bid_orders.keys()):
            del new_bid_orders[k]
        if normalized_position < 1.0 and np.isfinite(bid_price):
            for _ in range(grid_num):
                bid_price_tick = round(bid_price / tick_size)
                new_bid_orders[uint64(bid_price_tick)] = bid_price
                bid_price -= grid_interval

        for k in list(new_ask_orders.keys()):
            del new_ask_orders[k]
        if normalized_position > -1.0 and np.isfinite(ask_price):
            for _ in range(grid_num):
                ask_price_tick = round(ask_price / tick_size)
                new_ask_orders[uint64(ask_price_tick)] = ask_price
                ask_price += grid_interval

        order_values = orders.values()
        while order_values.has_next():
            order = order_values.get()
            if order.cancellable:
                if (
                    (order.side == BUY and order.order_id not in new_bid_orders)
                    or (order.side == SELL and order.order_id not in new_ask_orders)
                ):
                    hbt.cancel(asset_no, order.order_id, False)

        for order_id, order_price in new_bid_orders.items():
            if order_id not in orders:
                hbt.submit_buy_order(
                    asset_no, order_id, order_price, order_qty, GTX, LIMIT, False
                )

        for order_id, order_price in new_ask_orders.items():
            if order_id not in orders:
                hbt.submit_sell_order(
                    asset_no, order_id, order_price, order_qty, GTX, LIMIT, False
                )

        t += 1
        if t >= max_steps:
            break

    return float(np.nanmedian(half_spread_bps_arr))


def _resolve_price_param(value: float | None, ticks: float | None, tick_size: float) -> float:
    if value is not None:
        return float(value)
    if ticks is None:
        raise ValueError("missing ticks for price param")
    return float(ticks) * tick_size


def _resolve_scalar_param(value: float | None, fallback: float | None) -> float:
    if value is not None:
        return float(value)
    if fallback is None:
        raise ValueError("missing scalar param")
    return float(fallback)




def run_backtest(
    npz_path: Path,
    tick_size: float,
    lot_size: float,
    latency_ns: int,
    record_every: int,
    step_ns: int,
    window_steps: int,
    update_interval_steps: int,
    order_qty_dollar: float,
    max_position_dollar: float | None,
    max_position_multiplier: float,
    grid_num: int,
    vol_to_half_spread: float,
    half_spread: float | None,
    half_spread_bps: float,
    half_spread_ticks: float | None,
    skew: float | None,
    skew_ticks: float | None,
    c1: float | None,
    c1_ticks: float | None,
    grid_interval: float | None,
    grid_interval_ticks: float | None,
    looking_depth: float,
    roi_lb: float | None,
    roi_ub: float | None,
    roi_pad: float,
    plots_dir: Path | None,
    gap_threshold_minutes: float = 10.0,
    min_half_spread_bps: float = 1.0,
) -> None:
    try:
        from hftbacktest import (
            BacktestAsset,
            ROIVectorMarketDepthBacktest,
            BUY as HBUY,
            SELL as HSELL,
            GTX as HGTX,
            LIMIT as HLIMIT,
        )
        from hftbacktest.recorder import Recorder
    except Exception as exc:  # pragma: no cover - requires compiled extension
        raise RuntimeError(
            "hftbacktest extension is not available. Build/install py-hftbacktest before running the backtest."
        ) from exc

    if Dict is None:
        raise RuntimeError("numba is required to run the OBI strategy")

    global BUY, SELL, GTX, LIMIT
    BUY, SELL, GTX, LIMIT = HBUY, HSELL, HGTX, HLIMIT

    data = np.load(npz_path, mmap_mode='r')["data"]
    if len(data) == 0:
        print("no events in npz; skipping backtest")
        return

    if roi_lb is None or roi_ub is None:
        inferred_lb, inferred_ub = infer_roi_bounds(data, roi_pad)
        if roi_lb is None:
            roi_lb = inferred_lb
        if roi_ub is None:
            roi_ub = inferred_ub
    prices = data["px"].astype(np.float64)
    price_mask = np.isfinite(prices) & (prices > 0)
    sample_mid = float(np.nanmedian(prices[price_mask])) if np.any(price_mask) else np.nan

    roi_lb = float(np.floor(roi_lb / tick_size) * tick_size)
    roi_ub = float(np.ceil(roi_ub / tick_size) * tick_size)
    if roi_ub <= roi_lb:
        raise ValueError("roi bounds are invalid or too narrow")

    half_spread_value = 0.0
    if half_spread is not None or half_spread_ticks is not None:
        half_spread_value = _resolve_price_param(
            half_spread,
            half_spread_ticks,
            tick_size,
        )
        half_spread_bps = 0.0
        vol_to_half_spread = 0.0
    if half_spread_bps > 0:
        vol_to_half_spread = 0.0
    spread_mode = "fixed"
    sample_half_spread = half_spread_value
    if half_spread_bps > 0:
        spread_mode = "bps"
        if np.isfinite(sample_mid):
            sample_half_spread = sample_mid * (half_spread_bps / 10000.0)
        else:
            sample_half_spread = np.nan
    elif vol_to_half_spread > 0:
        spread_mode = "volatility"
        sample_half_spread = np.nan
    skew = _resolve_scalar_param(skew, skew_ticks)
    c1 = _resolve_price_param(c1, c1_ticks, tick_size)
    min_grid_step = _resolve_price_param(
        grid_interval,
        grid_interval_ticks,
        tick_size,
    )
    if min_grid_step <= 0:
        min_grid_step = tick_size
    min_grid_step = max(min_grid_step, tick_size)

    if order_qty_dollar <= 0:
        raise ValueError("order_qty_dollar must be > 0")
    if max_position_dollar is None or max_position_dollar <= 0:
        max_position_dollar = order_qty_dollar * max_position_multiplier

    step_ns = int(step_ns)
    if step_ns <= 0:
        raise ValueError("step_ns must be > 0")
    window_steps = max(1, int(window_steps))
    update_interval_steps = max(1, int(update_interval_steps))
    window_seconds = window_steps * step_ns / 1_000_000_000

    if record_every <= 0:
        record_every = 1
    params = compute_backtest_params(data, step_ns, record_every)
    max_steps = int(params["max_steps"])
    estimated = int(params["estimated"])
    base_ts_ns = int(params["base_ts_ns"])

    base_ts_ns, gap_starts_ns, gap_ends_ns, gap_log = build_gap_context(
        data, gap_threshold_minutes, base_ts_ns
    )
    if gap_log:
        print(gap_log)

    maker_fee, taker_fee = load_fees_from_config(Path("config.json"))
    print(
        "backtest_config:",
        f"step_ns={step_ns}",
        f"window_seconds={window_seconds}",
        f"window_steps={window_steps}",
        f"update_interval_steps={update_interval_steps}",
        f"record_every={record_every}",
        f"order_qty_dollar={order_qty_dollar}",
        f"max_position_dollar={max_position_dollar}",
        f"grid_num={grid_num}",
        f"half_spread={half_spread_value}",
        f"half_spread_bps={half_spread_bps}",
        f"spread_mode={spread_mode}",
        f"vol_to_half_spread={vol_to_half_spread}",
        f"min_half_spread_bps={min_half_spread_bps}",
        f"min_grid_step={min_grid_step}",
        f"sample_mid={sample_mid}",
        f"sample_half_spread={sample_half_spread}",
        f"skew={skew}",
        f"c1={c1}",
        f"looking_depth={looking_depth}",
        "grid_interval=dynamic",
        f"roi_lb={roi_lb}",
        f"roi_ub={roi_ub}",
        f"gap_threshold_minutes={gap_threshold_minutes}",
        f"maker_fee={maker_fee}",
        f"taker_fee={taker_fee}",
    )

    api = BacktestAPI(BacktestAsset, ROIVectorMarketDepthBacktest, Recorder)
    asset = build_asset(
        api,
        npz_path,
        tick_size,
        lot_size,
        latency_ns,
        maker_fee,
        taker_fee,
        roi_lb=roi_lb,
        roi_ub=roi_ub,
    )

    hbt = api.backtest_cls([asset])
    recorder = api.recorder_cls(1, estimated)

    median_spread_bps = obi_mm(
        hbt,
        recorder.recorder,
        record_every,
        step_ns,
        max_steps,
        vol_to_half_spread,
        min_grid_step,
        half_spread_value,
        half_spread_bps,
        skew,
        c1,
        looking_depth,
        window_steps,
        update_interval_steps,
        order_qty_dollar,
        max_position_dollar,
        grid_num,
        roi_lb,
        roi_ub,
        base_ts_ns,
        gap_starts_ns,
        gap_ends_ns,
        min_half_spread_bps,
    )

    hbt.close()

    records = recorder.get(0)
    if len(records) == 0:
        print("no records captured; backtest finished without emitting stats")
        return

    valid_mask = np.isfinite(records["price"])
    if np.any(valid_mask):
        last = records[np.where(valid_mask)[0][-1]]
        equity_wo_fee = float(last["balance"] + last["position"] * last["price"])
        equity = equity_wo_fee - float(last["fee"])
        max_pos = float(np.nanmax(np.abs(records["position"])))
        print(
            "backtest summary:",
            f"timestamp={int(last['timestamp'])}",
            f"price={float(last['price'])}",
            f"position={float(last['position'])}",
            f"balance={float(last['balance'])}",
            f"fee={float(last['fee'])}",
            f"equity_wo_fee={equity_wo_fee}",
            f"equity={equity}",
            f"num_trades={int(last['num_trades'])}",
            f"max_abs_position={max_pos}",
            f"median_spread_bps={median_spread_bps:.4f}",
        )
        if plots_dir is not None:
            save_plots(records[valid_mask], plots_dir, npz_path.stem)
    else:
        print("backtest summary: no finite price records found")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run an order book imbalance market-making backtest on parquet data."
    )
    default_symbol = load_symbol_from_config(Path("config.json"))
    add_common_args(parser, default_symbol)
    parser.add_argument("--out", default="data/btc_hft_obi.npz")
    add_backtest_args(parser, record_every_default=10, step_ns_default=100_000_000)
    parser.add_argument("--window-steps", type=int, default=6000)
    parser.add_argument("--update-interval-steps", type=int, default=50)
    parser.add_argument("--order-qty-dollar", type=float, default=20.0)
    parser.add_argument("--max-position-dollar", type=float, default=500.0)
    parser.add_argument("--max-position-multiplier", type=float, default=50.0)
    parser.add_argument("--grid-num", type=int, default=1)
    parser.add_argument("--vol-to-half-spread", type=float, default=32.0)
    parser.add_argument("--half-spread", type=float, default=None)
    parser.add_argument("--half-spread-bps", type=float, default=0.0)
    parser.add_argument("--half-spread-ticks", type=int, default=None)
    parser.add_argument("--min-half-spread-bps", type=float, default=1.0)
    parser.add_argument("--skew", type=float, default=0.5)
    parser.add_argument("--skew-ticks", type=float, default=None)
    parser.add_argument("--c1", type=float, default=None)
    parser.add_argument("--c1-ticks", type=int, default=605)
    parser.add_argument("--grid-interval", type=float, default=None)
    parser.add_argument("--grid-interval-ticks", type=int, default=1)
    parser.add_argument("--looking-depth", type=float, default=0.025)
    parser.add_argument("--roi-lb", type=float, default=None)
    parser.add_argument("--roi-ub", type=float, default=None)
    parser.add_argument("--roi-pad", type=float, default=0.02)
    add_run_backtest_args(parser)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    out_path = Path(args.out)
    print(f"data_dir={data_dir} out={out_path}")
    tick_size, lot_size, count, converted = convert_parquet_to_npz(
        data_dir, out_path, args.max_rows, args.latency_ns, args.symbol
    )
    if converted:
        print(f"conversion=created events={count}")
    else:
        print(f"conversion=skipped events={count}")
    print(
        f"tick_size={tick_size} lot_size={lot_size} latency_ns={args.latency_ns} max_rows={args.max_rows}"
    )
    meta = load_meta(out_path)
    if meta:
        print_meta_summary(meta, out_path)

    if args.run_backtest:
        plots_dir = Path(args.plots_dir) if args.plots_dir else None
        print("running_backtest=1")
        run_backtest(
            out_path,
            tick_size,
            lot_size,
            args.latency_ns,
            args.record_every,
            args.step_ns,
            args.window_steps,
            args.update_interval_steps,
            args.order_qty_dollar,
            args.max_position_dollar,
            args.max_position_multiplier,
            args.grid_num,
            args.vol_to_half_spread,
            args.half_spread,
            args.half_spread_bps,
            args.half_spread_ticks,
            args.skew,
            args.skew_ticks,
            args.c1,
            args.c1_ticks,
            args.grid_interval,
            args.grid_interval_ticks,
            args.looking_depth,
            args.roi_lb,
            args.roi_ub,
            args.roi_pad,
            plots_dir,
            args.gap_threshold_minutes,
            args.min_half_spread_bps,
        )


if __name__ == "__main__":
    main()
