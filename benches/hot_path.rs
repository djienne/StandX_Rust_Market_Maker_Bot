use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use standx_orderbook::trading::SharedEquity;
use standx_orderbook::{
    OrderManagerConfig, OrderbookSnapshot, PriceLevel, Quote, QuoteOrderManager, SharedPosition,
    Side, Symbol,
};

fn quote() -> Quote {
    Quote {
        symbol: Symbol::new("BTC-USD"),
        bid_prices: [99_999.0, 99_998.0],
        ask_prices: [100_001.0, 100_002.0],
        num_levels: 2,
        quantity: 0.00025,
        mid_price: 100_000.0,
        spread: 2.0,
        volatility: 0.001,
        alpha: 0.0,
        position: 0.0,
        half_spread_tick: 1.0,
        valid_for_trading: true,
        history_secs: 600.0,
        bid_floored: [false; 2],
        ask_floored: [false; 2],
    }
}

fn manager() -> QuoteOrderManager {
    let position = Arc::new(SharedPosition::new("BTC-USD"));
    position.set(0.0);
    let equity = Arc::new(SharedEquity::new(2, 10.0, 1.0));
    equity.set_equity(10_000.0);
    let config = OrderManagerConfig {
        symbol: "BTC-USD".to_string(),
        num_levels: 2,
        lot_size: 0.00001,
        min_order_qty: 0.00001,
        ..OrderManagerConfig::default()
    };
    QuoteOrderManager::new(config, position, equity)
}

fn snapshot(sequence: u64) -> OrderbookSnapshot {
    let mut snapshot = OrderbookSnapshot::new(Symbol::new("BTC-USD"));
    snapshot.sequence = sequence;
    snapshot.timestamp_ns = 1_800_000_000_000_000_000 + sequence as i64;
    snapshot.received_at_ns = snapshot.timestamp_ns;
    snapshot.bid_count = 20;
    snapshot.ask_count = 20;
    for index in 0..20 {
        snapshot.bids[index] = PriceLevel::new(100_000.0 - index as f64, 0.1);
        snapshot.asks[index] = PriceLevel::new(100_001.0 + index as f64, 0.1);
    }
    snapshot
}

fn bench_order_manager(c: &mut Criterion) {
    let quote = quote();

    c.bench_function("order_manager_initial_four_decisions", |b| {
        b.iter_batched(
            || (manager(), Vec::with_capacity(8)),
            |(mut manager, mut decisions)| {
                manager.on_quote_into(black_box(&quote), 1_000_000_000, &mut decisions);
                black_box(decisions)
            },
            BatchSize::SmallInput,
        )
    });

    let mut pending = manager();
    let decisions = pending.on_quote(&quote, 1_000_000_000);
    assert_eq!(decisions.len(), 4);
    let mut pending_decisions = Vec::with_capacity(8);
    c.bench_function("order_manager_pending_no_action", |b| {
        let mut now = 1_000_000_001_i64;
        b.iter(|| {
            now += 1;
            pending.on_quote_into(
                black_box(&quote),
                black_box(now),
                &mut pending_decisions,
            );
            black_box(pending_decisions.len())
        })
    });

    let mut live = manager();
    for decision in live.on_quote(&quote, 1_000_000_000) {
        if let standx_orderbook::OrderDecision::Send {
            side,
            level,
            cl_ord_id,
            ..
        } = decision
        {
            let order_id = match side {
                Side::Buy => 1_000 + level as i64,
                Side::Sell => 2_000 + level as i64,
            };
            live.on_order_accepted(&cl_ord_id, order_id);
        }
    }
    let mut live_decisions = Vec::with_capacity(8);
    c.bench_function("order_manager_live_no_action", |b| {
        let mut now = 1_000_000_001_i64;
        b.iter(|| {
            now += 1;
            live.on_quote_into(black_box(&quote), black_box(now), &mut live_decisions);
            black_box(live_decisions.len())
        })
    });
}

fn bench_snapshot(c: &mut Criterion) {
    let snapshot = snapshot(1);
    c.bench_function("snapshot_clone_20_levels", |b| {
        b.iter(|| black_box(black_box(&snapshot).clone()))
    });
}

fn bench_strategy(c: &mut Criterion) {
    let equity = Arc::new(SharedEquity::new(2, 10.0, 1.0));
    equity.set_equity(10_000.0);
    let config = standx_orderbook::StrategyConfig::default();
    let step = config.step_ns as i64;
    let mut strategy = standx_orderbook::ObiStrategy::new(config, None, Some(equity), None, 10, 5);
    let mut book = snapshot(1);
    for i in 0..=6000 { book.received_at_ns = 1 + i * step; strategy.update(&book); }
    assert!(strategy.is_valid_for_trading());
    c.bench_function("strategy_valid_depth_grid_update", |b| {
        b.iter(|| {
            book.received_at_ns += step;
            black_box(strategy.update(black_box(&book)))
        })
    });
}

fn depth_message(levels: usize) -> String {
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for i in 0..levels {
        bids.push(format!(r#"["{:.2}","{:.3}"]"#, 121_884.01 - i as f64 * 0.3, 0.001 + i as f64 * 0.01));
        asks.push(format!(r#"["{:.2}","{:.3}"]"#, 121_895.81 + i as f64 * 0.3, 0.843 + i as f64 * 0.01));
    }
    bids.reverse(); // exchange sends bids ascending (see DOCS/websocket.md)
    format!(
        r#"{{"seq":1,"channel":"depth_book","data":{{"symbol":"BTC-USD","asks":[{}],"bids":[{}],"sequence":123456,"time":"2025-08-11T03:44:40.922233Z"}}}}"#,
        asks.join(","), bids.join(",")
    )
}

fn bench_parse(c: &mut Criterion) {
    let text = depth_message(20);
    c.bench_function("parse_depth_20_levels_to_snapshot", |b| {
        b.iter(|| {
            match standx_orderbook::StandXMessage::parse_str(black_box(&text), 20, 1).unwrap() {
                standx_orderbook::StandXMessage::DepthBook(book) => black_box(book),
                _ => unreachable!(),
            }
        })
    });
}

criterion_group!(benches, bench_order_manager, bench_snapshot, bench_strategy, bench_parse);
criterion_main!(benches);
