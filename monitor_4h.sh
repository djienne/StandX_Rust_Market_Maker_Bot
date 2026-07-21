#!/bin/bash
while true; do
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  hits=$(grep -n "WARN\|ERROR\|panic\|Orderbook drift\|Crossed book\|Orderbook validation failed" /home/ubuntu/standx/run_4h.log 2>/dev/null | tail -n 20)
  sanity_hits=$(tail -n 2000 /home/ubuntu/standx/run_4h.log | grep "SANITY" | grep -v "diff: bid=0.00 ask=0.00" | tail -n 20)
  {
    printf "=== %s ===\n" "$ts"
    printf "[alerts]\n"
    if [ -n "$hits" ]; then printf "%s\n" "$hits"; else printf "(no hits)\n"; fi
    printf "[sanity_nonzero]\n"
    if [ -n "$sanity_hits" ]; then printf "%s\n" "$sanity_hits"; else printf "(no hits)\n"; fi
  } >> /home/ubuntu/standx/run_4h_watch.log
  sleep 600
done
