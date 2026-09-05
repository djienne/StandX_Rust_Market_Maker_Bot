# Operating checks

Use the deployment and observe-only commands in [README](README.md). Run only one live bot on a dedicated account: startup and reconciliation cancel account-wide orders. Do not run the historical leverage-test executable as a health check; it can change leverage.

## While running

```bash
docker compose logs --tail 100 bot
docker compose ps
```

Inspect fresh valid-depth activity, current position/equity age, warmup status, pause reasons, exchange order identities, and reconciliation failures. A running process or continuing ping/pong traffic does not establish healthy trading. Repeated empty REST polls cannot prove that an unacknowledged submission was rejected.

REST/WS price differences are diagnostics between observations at different times. The checker does not correct the trading book. Inspect depth freshness and timestamps before interpreting those differences.

Compose rotates logs at 50 MB with five files. Wallet CSVs are under `data/`; test-service CSVs are under `data/observe/`. The old `monitor_*`, `rotate_4h.sh`, and latency scripts refer to historical run paths and are not started by Compose.

## Stop and verify

```bash
docker compose stop bot
docker compose logs --tail 100 bot
```

SIGTERM invokes the same cleanup as Ctrl+C: permanently stop new submissions, settle outstanding submission outcomes, cancel orders, and verify zero open orders. Compose allows 120 seconds; final reconciliation has a 90-second budget. Positions are reported but not closed.

An unverified cleanup exits with an error and the service does not automatically restart. Inspect outstanding orders and positions directly at the exchange before restarting. An unresolved submission may have been accepted even if it is absent from the latest open-order response. Do not replace verification with repeated restarts.
