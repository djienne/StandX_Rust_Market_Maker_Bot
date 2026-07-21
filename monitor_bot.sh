#!/bin/bash
# Monitoring watcher for 1-hour test run
# Runs every 10 minutes, appends summaries to run_1h_watch.log

while true; do
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  # Get alerts
  hits=$(rg -n "WARN|ERROR|panic|Orderbook drift|Crossed book|Orderbook validation failed" run_1h.log 2>/dev/null | tail -n 20)

  # Get sanity diffs that are non-zero
  sanity_hits=$(tail -n 2000 run_1h.log 2>/dev/null | grep "SANITY" | grep -v "diff: bid=0.00 ask=0.00" | tail -n 20)

  {
    printf "=== %s ===\n" "$ts"
    if [ -n "$hits" ]; then
      printf "[alerts]\n%s\n" "$hits"
    else
      printf "[alerts]\n(no hits)\n"
    fi
    if [ -n "$sanity_hits" ]; then
      printf "[sanity_nonzero]\n%s\n" "$sanity_hits"
    else
      printf "[sanity_nonzero]\n(no hits)\n"
    fi
  } >> run_1h_watch.log

  sleep 600
done
