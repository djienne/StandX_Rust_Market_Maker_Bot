#!/bin/bash
while true; do
  size=$(stat -c %s /home/ubuntu/standx/run_4h.log 2>/dev/null || echo 0)
  max=$((50*1024*1024))
  if [ "$size" -ge "$max" ]; then
    ts=$(date -u +"%Y%m%dT%H%M%SZ")
    cp /home/ubuntu/standx/run_4h.log "/home/ubuntu/standx/run_4h.log.$ts"
    : > /home/ubuntu/standx/run_4h.log
    ls -1t /home/ubuntu/standx/run_4h.log.20* 2>/dev/null | tail -n +6 | xargs -r rm --
  fi
  sleep 300
done
