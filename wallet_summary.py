#!/usr/bin/env python3
"""Summarize wallet_history.csv trading performance."""

import csv
from datetime import datetime
from pathlib import Path

def parse_timestamp(ts: str) -> datetime:
    """Parse ISO timestamp, handling timezone."""
    # Remove timezone suffix for parsing
    ts = ts.split('+')[0].split('.')[0]
    return datetime.fromisoformat(ts)

def main():
    csv_path = Path(__file__).parent / "wallet_history.csv"

    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        return

    rows = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    if not rows:
        print("No data in CSV")
        return

    # Parse key values
    first = rows[0]
    last = rows[-1]

    start_time = parse_timestamp(first['timestamp'])
    end_time = parse_timestamp(last['timestamp'])
    duration = end_time - start_time

    start_equity = float(first['equity'])
    end_equity = float(last['equity'])
    pnl = end_equity - start_equity
    pnl_pct = (pnl / start_equity) * 100 if start_equity else 0

    total_volume_btc = float(last['total_volume'])
    total_volume_usd = float(last['total_volume_usd'])
    trade_count = int(last['trade_count'])

    # Find min/max equity
    equities = [float(r['equity']) for r in rows]
    min_equity = min(equities)
    max_equity = max(equities)
    max_drawdown = ((max_equity - min_equity) / max_equity) * 100 if max_equity else 0

    # Current position
    current_position = float(last['position_usd'])
    unrealized_pnl = float(last['unrealized_pnl'])

    # Print summary
    print("=" * 50)
    print("WALLET HISTORY SUMMARY")
    print("=" * 50)
    print(f"Period:         {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}")
    print(f"Duration:       {duration}")
    print(f"Data points:    {len(rows)}")
    print()
    print("EQUITY")
    print(f"  Start:        ${start_equity:.2f}")
    print(f"  End:          ${end_equity:.2f}")
    print(f"  Min:          ${min_equity:.2f}")
    print(f"  Max:          ${max_equity:.2f}")
    print()
    print("PERFORMANCE")
    print(f"  PnL:          ${pnl:+.2f} ({pnl_pct:+.4f}%)")
    print(f"  Max Drawdown: {max_drawdown:.4f}%")
    print()
    print("TRADING ACTIVITY")
    print(f"  Total Volume: {total_volume_btc:.4f} BTC (${total_volume_usd:.2f})")
    print(f"  Trade Count:  {trade_count}")
    if trade_count > 0:
        print(f"  Avg Trade:    ${total_volume_usd / trade_count:.2f}")
    print()
    print("CURRENT STATE")
    print(f"  Position:     ${current_position:.2f}")
    print(f"  Unrealized:   ${unrealized_pnl:+.2f}")
    print("=" * 50)

if __name__ == "__main__":
    main()
