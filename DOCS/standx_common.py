"""
StandX Common Module
Shared constants, base classes, and utilities for StandX WebSocket clients.
"""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import websockets

# =============================================================================
# Constants
# =============================================================================

WS_STREAM_URL = "wss://perps.standx.com/ws-stream/v1"
WS_API_URL = "wss://perps.standx.com/ws-api/v1"
AUTH_API_URL = "https://api.standx.com"
PERPS_API_URL = "https://perps.standx.com"

# WebSocket settings
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 10
RECONNECT_DELAY = 5

# Channels
CHANNEL_DEPTH_BOOK = "depth_book"
CHANNEL_PUBLIC_TRADE = "public_trade"
CHANNEL_PRICE = "price"
CHANNEL_BALANCE = "balance"
CHANNEL_ORDER = "order"
CHANNEL_POSITION = "position"
CHANNEL_TRADE = "trade"


# =============================================================================
# Logging Setup
# =============================================================================

def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure logging with standard format."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


# Module-level logger
logger = setup_logging()


# =============================================================================
# Utility Functions
# =============================================================================

def parse_timestamp(ts: Any) -> datetime:
    """
    Parse timestamp from server response.
    Handles Unix milliseconds, ISO strings, or returns current UTC time.
    """
    if ts is None:
        return datetime.now(timezone.utc)
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    if isinstance(ts, str):
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    return datetime.now(timezone.utc)


def format_price(price: Any, decimals: int = 2) -> str:
    """Format price with commas and specified decimals."""
    if price is None:
        return "N/A"
    try:
        return f"{float(price):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(price)


def format_quantity(qty: Any, decimals: int = 4) -> str:
    """Format quantity with specified decimals."""
    if qty is None:
        return "N/A"
    try:
        return f"{float(qty):.{decimals}f}"
    except (ValueError, TypeError):
        return str(qty)


# =============================================================================
# Orderbook Utilities
# =============================================================================

def sort_orderbook(
    bids: List,
    asks: List,
    levels: int = 20
) -> tuple[List, List]:
    """
    Sort and limit orderbook to specified levels.
    Returns (bids_sorted, asks_sorted) where:
    - bids are sorted descending (best bid first)
    - asks are sorted ascending (best ask first)
    """
    asks_sorted = sorted(asks, key=lambda x: float(x[0]))[:levels]
    bids_sorted = sorted(bids, key=lambda x: float(x[0]), reverse=True)[:levels]
    return bids_sorted, asks_sorted


def validate_orderbook(bids: List, asks: List, symbol: str = "") -> tuple[bool, List[str]]:
    """
    Validate orderbook data consistency.
    Returns (is_valid, issues_list).
    """
    issues = []

    if not bids or not asks:
        issues.append("Empty bids or asks")
        return False, issues

    try:
        bid_prices = [float(b[0]) for b in bids]
        ask_prices = [float(a[0]) for a in asks]
        bid_qtys = [float(b[1]) for b in bids]
        ask_qtys = [float(a[1]) for a in asks]

        # Check bid ordering (should be descending)
        for i in range(len(bid_prices) - 1):
            if bid_prices[i] < bid_prices[i + 1]:
                issues.append(f"Bids not descending at {i}: {bid_prices[i]} < {bid_prices[i+1]}")

        # Check ask ordering (should be ascending)
        for i in range(len(ask_prices) - 1):
            if ask_prices[i] > ask_prices[i + 1]:
                issues.append(f"Asks not ascending at {i}: {ask_prices[i]} > {ask_prices[i+1]}")

        # Check for crossed book (bid > ask is invalid, bid == ask is locked market)
        best_bid = max(bid_prices)
        best_ask = min(ask_prices)
        if best_bid > best_ask:
            issues.append(f"Crossed book: best_bid={best_bid} > best_ask={best_ask}")

        # Check for positive quantities
        if any(q <= 0 for q in bid_qtys):
            issues.append("Negative or zero bid quantity")
        if any(q <= 0 for q in ask_qtys):
            issues.append("Negative or zero ask quantity")

    except (ValueError, IndexError) as e:
        issues.append(f"Parse error: {e}")

    is_valid = len(issues) == 0
    if not is_valid and symbol:
        logger.warning(f"Orderbook validation issues for {symbol}: {issues}")

    return is_valid, issues


def calculate_spread(best_bid: Optional[float], best_ask: Optional[float]) -> Optional[float]:
    """Calculate bid-ask spread."""
    if best_bid is not None and best_ask is not None:
        return best_ask - best_bid
    return None


def calculate_mid_price(best_bid: Optional[float], best_ask: Optional[float]) -> Optional[float]:
    """Calculate mid price."""
    if best_bid is not None and best_ask is not None:
        return (best_ask + best_bid) / 2
    return None


# =============================================================================
# Base WebSocket Client
# =============================================================================

class BaseWebSocketClient(ABC):
    """
    Abstract base class for StandX WebSocket clients.
    Provides common connection, reconnection, health check, and message handling logic.
    """

    # Reconnection settings
    MAX_RECONNECT_DELAY = 60  # Max backoff delay in seconds
    STALE_CONNECTION_TIMEOUT = 60  # Force reconnect if no message for this many seconds

    def __init__(
        self,
        url: str = WS_STREAM_URL,
        ping_interval: int = WS_PING_INTERVAL,
        ping_timeout: int = WS_PING_TIMEOUT,
        reconnect_delay: int = RECONNECT_DELAY
    ):
        self.url = url
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.reconnect_delay = reconnect_delay
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self._stopped = False  # Explicit stop flag
        self._logger = logging.getLogger(self.__class__.__name__)

        # Health check state
        self._last_message_time: Optional[datetime] = None
        self._health_check_task: Optional[asyncio.Task] = None

        # Connection statistics
        self._stats = {
            "messages_received": 0,
            "reconnect_count": 0,
            "last_connect_time": None,
            "total_uptime_seconds": 0,
        }

    @property
    def stats(self) -> Dict:
        """Get connection statistics."""
        return self._stats.copy()

    @property
    def is_healthy(self) -> bool:
        """Check if connection is healthy (received message recently)."""
        if not self.running or not self._last_message_time:
            return False
        elapsed = (datetime.now(timezone.utc) - self._last_message_time).total_seconds()
        return elapsed < self.STALE_CONNECTION_TIMEOUT

    async def connect(self):
        """Establish WebSocket connection."""
        self._logger.info(f"Connecting to {self.url}")
        self.ws = await websockets.connect(
            self.url,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout
        )
        self.running = True
        self._last_message_time = datetime.now(timezone.utc)
        self._stats["last_connect_time"] = self._last_message_time
        self._logger.info("Connected successfully")

    @abstractmethod
    async def subscribe(self):
        """Subscribe to channels. Must be implemented by subclasses."""
        pass

    @abstractmethod
    async def handle_message(self, data: Dict) -> Any:
        """Handle incoming message. Must be implemented by subclasses."""
        pass

    async def _health_check_loop(self):
        """Monitor connection health and force reconnect if stale."""
        while self.running and not self._stopped:
            await asyncio.sleep(10)  # Check every 10 seconds

            if not self._last_message_time:
                continue

            elapsed = (datetime.now(timezone.utc) - self._last_message_time).total_seconds()
            if elapsed > self.STALE_CONNECTION_TIMEOUT:
                self._logger.warning(
                    f"Connection stale: no message for {elapsed:.0f}s, forcing reconnect"
                )
                self.running = False
                if self.ws:
                    await self.ws.close()
                break

    async def listen(self, callback: Optional[Callable] = None):
        """Listen for messages and dispatch to handler."""
        # Start health check task
        self._health_check_task = asyncio.create_task(self._health_check_loop())

        try:
            async for message in self.ws:
                # Update health check timestamp
                self._last_message_time = datetime.now(timezone.utc)
                self._stats["messages_received"] += 1

                data = json.loads(message)

                if "error" in data:
                    self._logger.error(f"Error received: {data}")
                    continue

                result = await self.handle_message(data)

                if callback and result is not None:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(result)
                    else:
                        callback(result)

        except websockets.ConnectionClosed as e:
            self._logger.warning(f"Connection closed: {e}")
        except Exception as e:
            self._logger.error(f"Error in listener: {e}")
        finally:
            self.running = False
            # Cancel health check task
            if self._health_check_task:
                self._health_check_task.cancel()
                try:
                    await self._health_check_task
                except asyncio.CancelledError:
                    pass

    async def run(self, callback: Optional[Callable] = None):
        """Main run loop with automatic reconnection and exponential backoff."""
        current_delay = self.reconnect_delay
        connect_time = None

        while not self._stopped:
            try:
                await self.connect()
                connect_time = datetime.now(timezone.utc)
                await self.subscribe()
                current_delay = self.reconnect_delay  # Reset backoff on successful connection
                await self.listen(callback)
            except Exception as e:
                self._logger.error(f"Connection error: {e}")

            # Update uptime stats
            if connect_time:
                session_duration = (datetime.now(timezone.utc) - connect_time).total_seconds()
                self._stats["total_uptime_seconds"] += session_duration

            # Check if we should stop
            if self._stopped:
                break

            # Reconnect with exponential backoff
            self._stats["reconnect_count"] += 1
            self._logger.info(
                f"Reconnecting in {current_delay}s... "
                f"(attempt #{self._stats['reconnect_count']})"
            )
            await asyncio.sleep(current_delay)

            # Exponential backoff with max limit
            current_delay = min(current_delay * 2, self.MAX_RECONNECT_DELAY)

    async def close(self):
        """Close the WebSocket connection and stop reconnection."""
        self._stopped = True
        self.running = False
        if self.ws:
            await self.ws.close()
            self._logger.info("Connection closed")

    def stop(self):
        """Signal the client to stop (synchronous version)."""
        self._stopped = True
        self.running = False

    def _create_subscribe_msg(self, channel: str, symbol: Optional[str] = None) -> Dict:
        """Create subscription message."""
        msg = {"subscribe": {"channel": channel}}
        if symbol:
            msg["subscribe"]["symbol"] = symbol
        return msg

    def _create_unsubscribe_msg(self, channel: str, symbol: Optional[str] = None) -> Dict:
        """Create unsubscription message."""
        msg = {"unsubscribe": {"channel": channel}}
        if symbol:
            msg["unsubscribe"]["symbol"] = symbol
        return msg

    async def send_subscribe(self, channel: str, symbol: Optional[str] = None):
        """Send subscription message."""
        msg = self._create_subscribe_msg(channel, symbol)
        await self.ws.send(json.dumps(msg))
        self._logger.info(f"Subscribed to {channel}" + (f" for {symbol}" if symbol else ""))

    async def send_unsubscribe(self, channel: str, symbol: Optional[str] = None):
        """Send unsubscription message."""
        msg = self._create_unsubscribe_msg(channel, symbol)
        await self.ws.send(json.dumps(msg))
        self._logger.info(f"Unsubscribed from {channel}" + (f" for {symbol}" if symbol else ""))


# =============================================================================
# Parquet Utilities
# =============================================================================

def flush_buffer_to_parquet(
    buffer: List[Dict],
    output_dir: Path,
    file_prefix: str,
    symbol_column: str = "symbol",
    dedupe_columns: Optional[List[str]] = None
) -> int:
    """
    Flush a buffer of records to Parquet files, grouped by symbol.
    Optionally removes duplicates based on specified columns.
    Returns count of records flushed.
    """
    if not buffer:
        return 0

    import pandas as pd

    try:
        df = pd.DataFrame(buffer)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        total_flushed = 0

        for symbol in df[symbol_column].unique():
            symbol_df = df[df[symbol_column] == symbol].copy()
            filename = output_dir / f"{file_prefix}_{symbol}_{date_str}.parquet"

            if filename.exists():
                existing_df = pd.read_parquet(filename)
                combined_df = pd.concat([existing_df, symbol_df], ignore_index=True)

                # Remove duplicates if dedupe_columns specified
                if dedupe_columns:
                    before_count = len(combined_df)
                    combined_df = combined_df.drop_duplicates(subset=dedupe_columns, keep='first')
                    dupes_removed = before_count - len(combined_df)
                    if dupes_removed > 0:
                        logger.info(f"Removed {dupes_removed} duplicate records from {filename.name}")

                combined_df.to_parquet(filename, index=False)
                records_added = len(combined_df) - len(existing_df)
            else:
                # Remove duplicates within new data
                if dedupe_columns:
                    symbol_df = symbol_df.drop_duplicates(subset=dedupe_columns, keep='first')
                symbol_df.to_parquet(filename, index=False)
                records_added = len(symbol_df)

            total_flushed += records_added
            logger.info(f"Flushed {records_added} records to {filename}")

        return total_flushed

    except Exception as e:
        logger.error(f"Error flushing {file_prefix} buffer: {e}")
        return 0


# =============================================================================
# Config Loader
# =============================================================================

class Config:
    """Configuration loader from JSON file."""

    def __init__(self, config_path: str = "config.json"):
        with open(config_path, 'r') as f:
            data = json.load(f)

        self.symbols: List[str] = data.get("symbols", ["BTC-USD"])
        self.orderbook_levels: int = data.get("orderbook_levels", 20)
        self.flush_interval_seconds: int = data.get("flush_interval_seconds", 5)
        self.output_dir: Path = Path(data.get("output_dir", "./data"))

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Config loaded: symbols={self.symbols}, levels={self.orderbook_levels}, "
            f"flush_interval={self.flush_interval_seconds}s, output_dir={self.output_dir}"
        )

    @classmethod
    def from_dict(cls, data: Dict) -> 'Config':
        """Create Config from dictionary."""
        config = cls.__new__(cls)
        config.symbols = data.get("symbols", ["BTC-USD"])
        config.orderbook_levels = data.get("orderbook_levels", 20)
        config.flush_interval_seconds = data.get("flush_interval_seconds", 5)
        config.output_dir = Path(data.get("output_dir", "./data"))
        config.output_dir.mkdir(parents=True, exist_ok=True)
        return config
