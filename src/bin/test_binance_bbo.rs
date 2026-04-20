//! Integration test for Binance bookTicker BBO feed.
//!
//! Connects to the real Binance Futures bookTicker WebSocket stream,
//! validates message parsing, and exercises the SharedBbo atomic storage.
//!
//! Run: cargo run --release --bin test_binance_bbo

use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use standx_orderbook::binance::{
    BinanceBookTickerClient, BookTickerClientConfig, BookTickerEvent, SharedBbo,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    println!("=== Binance BookTicker BBO Test ===\n");

    // Create client using the real code path
    let config = BookTickerClientConfig::btcusdt();
    let client = Arc::new(BinanceBookTickerClient::with_config(config.clone()));
    let shared_bbo = Arc::new(SharedBbo::new());

    println!("Connecting to Binance bookTicker WebSocket...");
    println!("URL: {}", config.ws_stream_url());
    println!("Symbol: BTCUSDT (Futures)\n");

    // Start the client (real WebSocket connection)
    let mut rx = client.clone().run().await;

    let mut msg_count: u64 = 0;
    let mut parse_errors: u64 = 0;
    let mut last_print = std::time::Instant::now();

    let test_duration = Duration::from_secs(15);
    let start = std::time::Instant::now();

    println!("Collecting data for 15 seconds...\n");
    println!(
        "{:>8} {:>12} {:>12} {:>12} {:>10} {:>10} {:>10}",
        "Count", "Bid", "Ask", "Mid", "Spread", "BidQty", "AskQty"
    );
    println!("{}", "-".repeat(84));

    while start.elapsed() < test_duration {
        match timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(event)) => match event {
                BookTickerEvent::Update(ticker) => {
                    // Parse using real BinanceBookTicker methods
                    let bid = match ticker.bid_price_f64() {
                        Ok(v) => v,
                        Err(e) => {
                            parse_errors += 1;
                            eprintln!("Parse error (bid): {}", e);
                            continue;
                        }
                    };
                    let ask = match ticker.ask_price_f64() {
                        Ok(v) => v,
                        Err(e) => {
                            parse_errors += 1;
                            eprintln!("Parse error (ask): {}", e);
                            continue;
                        }
                    };
                    let bid_qty = match ticker.bid_qty_f64() {
                        Ok(v) => v,
                        Err(e) => {
                            parse_errors += 1;
                            eprintln!("Parse error (bid_qty): {}", e);
                            continue;
                        }
                    };
                    let ask_qty = match ticker.ask_qty_f64() {
                        Ok(v) => v,
                        Err(e) => {
                            parse_errors += 1;
                            eprintln!("Parse error (ask_qty): {}", e);
                            continue;
                        }
                    };

                    msg_count += 1;

                    // Validate sanity
                    assert!(bid > 0.0, "Bid must be positive, got {}", bid);
                    assert!(ask > bid, "Ask must be > bid: ask={} bid={}", ask, bid);
                    assert!(bid_qty > 0.0, "Bid qty must be positive, got {}", bid_qty);
                    assert!(ask_qty > 0.0, "Ask qty must be positive, got {}", ask_qty);

                    // Update SharedBbo using real code path
                    shared_bbo.update(bid, ask, bid_qty, ask_qty, ticker.update_id);

                    // Verify SharedBbo reads back correct values
                    assert!(
                        (shared_bbo.best_bid() - bid).abs() < 1e-10,
                        "SharedBbo bid mismatch"
                    );
                    assert!(
                        (shared_bbo.best_ask() - ask).abs() < 1e-10,
                        "SharedBbo ask mismatch"
                    );
                    assert!(shared_bbo.is_valid(), "SharedBbo should be valid after update");
                    assert!(
                        !shared_bbo.is_stale(1000),
                        "SharedBbo should not be stale right after update"
                    );

                    // Print summary every second
                    if last_print.elapsed() >= Duration::from_secs(1) {
                        let mid = shared_bbo.mid();
                        let spread = shared_bbo.spread();

                        println!(
                            "{:>8} {:>12.2} {:>12.2} {:>12.2} {:>10.2} {:>10.3} {:>10.3}",
                            msg_count, bid, ask, mid, spread, bid_qty, ask_qty
                        );

                        last_print = std::time::Instant::now();
                    }
                }
                BookTickerEvent::Disconnected(reason) => {
                    println!("\n[DISCONNECTED] {}\n", reason);
                }
                BookTickerEvent::Error(e) => {
                    println!("\n[ERROR] {}\n", e);
                }
            },
            Ok(None) => {
                println!("Channel closed");
                break;
            }
            Err(_) => {
                println!("Timeout waiting for event");
            }
        }
    }

    // Stop the client
    client.stop();

    // Print summary
    let elapsed = start.elapsed().as_secs_f64();
    let stats = client.stats().snapshot();

    println!("\n{}", "=".repeat(84));
    println!("=== Summary ===\n");
    println!("Total messages received: {}", msg_count);
    println!("Parse errors: {}", parse_errors);
    println!("Message rate: {:.1} msg/sec", msg_count as f64 / elapsed);
    println!("Bytes received: {} KB", stats.bytes_received / 1024);
    println!("Reconnects: {}", stats.reconnects);

    println!("\nFinal BBO (from SharedBbo):");
    println!("  Best bid: {:.2}", shared_bbo.best_bid());
    println!("  Best ask: {:.2}", shared_bbo.best_ask());
    println!("  Mid:      {:.2}", shared_bbo.mid());
    println!("  Spread:   {:.2}", shared_bbo.spread());
    println!("  Valid:    {}", shared_bbo.is_valid());
    println!("  Stale:    {}", shared_bbo.is_stale(5000));

    // Final assertions
    assert!(msg_count > 0, "Should have received at least one message");
    assert_eq!(parse_errors, 0, "Should have zero parse errors");
    assert!(
        msg_count as f64 / elapsed > 1.0,
        "Should receive > 1 msg/sec for BTCUSDT"
    );

    println!("\nAll assertions passed!");

    Ok(())
}
