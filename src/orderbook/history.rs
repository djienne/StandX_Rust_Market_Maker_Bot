//! Thread-safe, pre-allocated ring buffer for diagnostic orderbook history.

use parking_lot::RwLock;

use crate::types::{OrderbookSnapshot, Symbol};

struct HistoryState {
    buffer: Box<[OrderbookSnapshot]>,
    initialized: Box<[bool]>,
    write_pos: u64,
    total_writes: u64,
}

/// Provides O(1) writes and owned diagnostic reads. The main loop records into
/// this structure only after order decisions have been dispatched.
pub struct OrderbookHistory {
    state: RwLock<HistoryState>,
    capacity: usize,
    retention_ns: u64,
}

impl OrderbookHistory {
    /// Create a new orderbook history buffer.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Number of snapshots to store
    /// * `retention_minutes` - How long to retain snapshots (for filtering)
    pub fn new(capacity: usize, retention_minutes: u64) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");

        Self {
            state: RwLock::new(HistoryState {
                buffer: vec![OrderbookSnapshot::default(); capacity].into_boxed_slice(),
                initialized: vec![false; capacity].into_boxed_slice(),
                write_pos: 0,
                total_writes: 0,
            }),
            capacity,
            retention_ns: retention_minutes * 60 * 1_000_000_000,
        }
    }

    /// Push a new snapshot into the buffer.
    ///
    /// Overwrites the oldest entry when the buffer is full.
    pub fn push(&self, snapshot: OrderbookSnapshot) {
        let mut state = self.state.write();
        let pos = state.write_pos;
        let idx = (pos as usize) % self.capacity;
        state.buffer[idx] = snapshot;
        state.initialized[idx] = true;
        state.write_pos = pos.wrapping_add(1);
        state.total_writes = state.total_writes.wrapping_add(1);
    }

    /// Get the most recent snapshot.
    ///
    /// Returns `None` if no snapshots have been written.
    pub fn latest(&self) -> Option<OrderbookSnapshot> {
        let state = self.state.read();
        if state.write_pos == 0 {
            return None;
        }
        let idx = ((state.write_pos - 1) as usize) % self.capacity;
        state.initialized[idx].then(|| state.buffer[idx].clone())
    }

    /// Get the most recent snapshot for a specific symbol.
    pub fn latest_for_symbol(&self, symbol: &Symbol) -> Option<OrderbookSnapshot> {
        let state = self.state.read();
        let pos = state.write_pos;
        if pos == 0 {
            return None;
        }

        // Search backwards from the most recent
        let start = pos.saturating_sub(1);
        let search_count = self.capacity.min(pos as usize);

        for i in 0..search_count {
            let idx = ((start - i as u64) as usize) % self.capacity;
            if state.initialized[idx] && &state.buffer[idx].symbol == symbol {
                return Some(state.buffer[idx].clone());
            }
        }

        None
    }

    /// Iterate over snapshots within the retention window.
    ///
    /// Returns an iterator yielding snapshots from newest to oldest.
    /// Snapshots that were being written during iteration are skipped.
    pub fn recent_snapshots(&self) -> impl Iterator<Item = OrderbookSnapshot> {
        let now_ns = self.current_time_ns();
        let cutoff_ns = now_ns.saturating_sub(self.retention_ns as i64);
        let state = self.state.read();
        let pos = state.write_pos;
        let count = self.capacity.min(pos as usize);
        let mut snapshots = Vec::with_capacity(count);
        for offset in 0..count {
            let idx = ((pos - 1 - offset as u64) as usize) % self.capacity;
            if !state.initialized[idx] {
                continue;
            }
            let snapshot = &state.buffer[idx];
            if snapshot.timestamp_ns < cutoff_ns {
                break;
            }
            snapshots.push(snapshot.clone());
        }
        snapshots.into_iter()
    }

    /// Get snapshots for a specific symbol within the retention window.
    pub fn recent_for_symbol(&self, symbol: &Symbol) -> Vec<OrderbookSnapshot> {
        self.recent_snapshots()
            .filter(|s| &s.symbol == symbol)
            .collect()
    }

    /// Get the current time in nanoseconds since epoch.
    fn current_time_ns(&self) -> i64 {
        // Use system time for absolute timestamps
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }

    /// Get the number of snapshots currently in the buffer.
    pub fn len(&self) -> usize {
        self.capacity.min(self.state.read().write_pos as usize)
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.state.read().write_pos == 0
    }

    /// Get the total number of writes since creation.
    pub fn total_writes(&self) -> u64 {
        self.state.read().total_writes
    }

    /// Get the buffer capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the retention window in minutes.
    pub fn retention_minutes(&self) -> u64 {
        self.retention_ns / 60 / 1_000_000_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_push_and_latest() {
        let history = OrderbookHistory::new(100, 10);

        assert!(history.is_empty());
        assert!(history.latest().is_none());

        let mut snapshot = OrderbookSnapshot::new(Symbol::new("TEST-USD"));
        snapshot.sequence = 1;
        snapshot.timestamp_ns = 1000;
        history.push(snapshot);

        assert!(!history.is_empty());
        assert_eq!(history.len(), 1);

        let latest = history.latest().unwrap();
        assert_eq!(latest.sequence, 1);
    }

    #[test]
    fn test_wrap_around() {
        let history = OrderbookHistory::new(10, 10);

        // Write more than capacity
        for i in 0..25 {
            let mut snapshot = OrderbookSnapshot::new(Symbol::new("TEST-USD"));
            snapshot.sequence = i;
            history.push(snapshot);
        }

        assert_eq!(history.len(), 10); // Capped at capacity
        assert_eq!(history.total_writes(), 25);

        // Latest should be the last written
        let latest = history.latest().unwrap();
        assert_eq!(latest.sequence, 24);
    }

    #[test]
    fn test_symbol_filter() {
        let history = OrderbookHistory::new(100, 10);

        let mut btc = OrderbookSnapshot::new(Symbol::new("TEST-USD"));
        btc.sequence = 1;
        history.push(btc);

        let mut eth = OrderbookSnapshot::new(Symbol::new("ETH-USD"));
        eth.sequence = 2;
        history.push(eth);

        let latest_btc = history.latest_for_symbol(&Symbol::new("TEST-USD")).unwrap();
        assert_eq!(latest_btc.sequence, 1);

        let latest_eth = history.latest_for_symbol(&Symbol::new("ETH-USD")).unwrap();
        assert_eq!(latest_eth.sequence, 2);
    }

    #[test]
    fn concurrent_push_and_read_is_safe() {
        let history = Arc::new(OrderbookHistory::new(256, 10));
        let mut threads = Vec::new();

        for writer in 0..2_u64 {
            let history = Arc::clone(&history);
            threads.push(std::thread::spawn(move || {
                for sequence in 1..=5_000_u64 {
                    let mut snapshot = OrderbookSnapshot::new(Symbol::new("TEST-USD"));
                    snapshot.sequence = writer * 5_000 + sequence;
                    history.push(snapshot);
                }
            }));
        }

        for _ in 0..4 {
            let history = Arc::clone(&history);
            threads.push(std::thread::spawn(move || {
                for _ in 0..5_000 {
                    if let Some(snapshot) = history.latest() {
                        assert!(snapshot.sequence > 0);
                    }
                }
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(history.total_writes(), 10_000);
        assert_eq!(history.len(), 256);
    }
}
