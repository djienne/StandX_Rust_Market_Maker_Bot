//! Open orders checker for detecting stale internal state.
//!
//! Polls the exchange for open orders and signals when internal state
//! should be cleared (exchange has no orders but we think we do).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

use super::auth::AuthManager;

/// Signal sent when internal order state should be cleared.
#[derive(Debug, Clone)]
pub struct ClearOrdersSignal {
    /// Symbol to clear orders for
    pub symbol: String,
    /// Reason for clearing
    pub reason: String,
}

/// Open orders checker configuration.
#[derive(Debug, Clone)]
pub struct OpenOrdersCheckerConfig {
    /// Polling interval
    pub interval: Duration,
    /// Symbol to check
    pub symbol: String,
    /// Number of consecutive zero-order polls before signaling (debounce)
    pub debounce_count: u32,
}

impl Default for OpenOrdersCheckerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3),
            symbol: "BTC-USD".to_string(),
            debounce_count: 2, // 2 consecutive polls = 6 seconds
        }
    }
}

/// Background open orders checker.
///
/// Polls the exchange for open orders and sends a signal when the exchange
/// reports 0 orders. This allows the main loop to immediately clear internal
/// state and place new orders, instead of waiting for timeout.
pub struct OpenOrdersChecker {
    /// Auth manager for API calls
    auth: Arc<Mutex<AuthManager>>,
    /// Configuration
    config: OpenOrdersCheckerConfig,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Channel to send clear signals
    signal_tx: mpsc::Sender<ClearOrdersSignal>,
}

impl OpenOrdersChecker {
    /// Create a new open orders checker.
    pub fn new(
        auth: Arc<Mutex<AuthManager>>,
        config: OpenOrdersCheckerConfig,
        signal_tx: mpsc::Sender<ClearOrdersSignal>,
    ) -> Self {
        Self {
            auth,
            config,
            running: Arc::new(AtomicBool::new(false)),
            signal_tx,
        }
    }

    /// Start the checker as a background task.
    ///
    /// Returns a handle that can be used to stop the checker.
    pub fn start(self) -> OpenOrdersCheckerHandle {
        let running = Arc::clone(&self.running);
        running.store(true, Ordering::Release);

        let handle = tokio::spawn(async move {
            self.run().await;
        });

        OpenOrdersCheckerHandle {
            running,
            task: handle,
        }
    }

    /// Run the polling loop.
    async fn run(self) {
        info!(
            "[{}] Open orders checker started (interval: {:?}, debounce: {})",
            self.config.symbol, self.config.interval, self.config.debounce_count
        );

        let mut consecutive_zero_count = 0u32;
        let mut consecutive_errors = 0u32;
        let mut last_order_count = 0usize;

        while self.running.load(Ordering::Acquire) {
            match self.poll_open_orders().await {
                Ok(order_count) => {
                    consecutive_errors = 0;

                    if order_count == 0 {
                        consecutive_zero_count += 1;

                        // Only signal after debounce threshold
                        if consecutive_zero_count >= self.config.debounce_count {
                            // Only signal once per "no orders" period
                            if last_order_count > 0 || consecutive_zero_count == self.config.debounce_count {
                                info!(
                                    "[{}] Exchange has 0 open orders (confirmed {} times) - signaling clear",
                                    self.config.symbol, consecutive_zero_count
                                );

                                let signal = ClearOrdersSignal {
                                    symbol: self.config.symbol.clone(),
                                    reason: format!("Exchange confirmed 0 orders ({} polls)", consecutive_zero_count),
                                };

                                if let Err(e) = self.signal_tx.send(signal).await {
                                    warn!("[{}] Failed to send clear signal: {}", self.config.symbol, e);
                                }
                            }
                        } else {
                            debug!(
                                "[{}] Exchange has 0 orders (count: {}/{})",
                                self.config.symbol, consecutive_zero_count, self.config.debounce_count
                            );
                        }
                    } else {
                        // Orders exist on exchange
                        if consecutive_zero_count > 0 {
                            debug!(
                                "[{}] Exchange has {} orders (resetting zero count from {})",
                                self.config.symbol, order_count, consecutive_zero_count
                            );
                        }
                        consecutive_zero_count = 0;
                    }

                    last_order_count = order_count;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    if consecutive_errors <= 3 {
                        warn!(
                            "[{}] Open orders check failed (attempt {}): {}",
                            self.config.symbol, consecutive_errors, e
                        );
                    } else if consecutive_errors % 10 == 0 {
                        error!(
                            "[{}] Open orders check failing repeatedly ({} errors): {}",
                            self.config.symbol, consecutive_errors, e
                        );
                    }
                }
            }

            // Sleep until next poll
            tokio::time::sleep(self.config.interval).await;
        }

        info!("[{}] Open orders checker stopped", self.config.symbol);
    }

    /// Poll open orders from API.
    async fn poll_open_orders(&self) -> Result<usize, String> {
        let mut auth = self.auth.lock().await;

        let open_orders = auth
            .query_open_orders(Some(&self.config.symbol))
            .await
            .map_err(|e| e.to_string())?;

        Ok(open_orders.len())
    }
}

/// Handle to control a running open orders checker.
pub struct OpenOrdersCheckerHandle {
    running: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl OpenOrdersCheckerHandle {
    /// Stop the checker.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Wait for the checker to stop.
    pub async fn join(self) {
        self.stop();
        let _ = self.task.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = OpenOrdersCheckerConfig::default();
        assert_eq!(config.interval, Duration::from_secs(3));
        assert_eq!(config.debounce_count, 2);
    }
}
