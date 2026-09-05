# Repository working instructions

Use Cargo's release profile for builds, checks, lints, tests, and benchmarks:

```bash
cargo build --release --locked --bin standx-orderbook
cargo test --release --lib --bin standx-orderbook --locked
cargo check --release --all-targets --locked
cargo clippy --release --all-targets --locked
cargo bench --profile release --bench hot_path --locked
```

`cargo bench` selects release with `--profile release`; it does not accept `--release`. Do not create debug-profile artifacts for this project.

Keep changes focused on trading behavior and evidence. Preserve concurrent work. Do not run the live configuration as a test: it submits real orders and may change leverage. Do not expose `.env` or credentials in logs or build contexts.

The README describes deployment, supported symbols, and safety behavior. SPREAD_CALCULATION.md is the single explanation of sampling, pricing, and sizing. Source and configuration are authoritative. Avoid duplicating these descriptions here.

Keep one risk-checking path for initial orders and replacements. A pause invalidates queued generations; cleanup waits behind the submission barrier. Do not forget unresolved submissions on disconnect or infer rejection from an empty REST snapshot. Current-book storage is diagnostic and uses a lock; do not reintroduce unsafe snapshot sharing for speculative latency improvements.
