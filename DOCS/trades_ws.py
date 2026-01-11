"""
StandX WebSocket Trades Client
Subscribes to public_trade channel for real-time trade data
"""

import asyncio
import json
import logging
from collections import deque
from datetime import datetime
from typing import Callable, Dict, List, Optional
import websockets

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TradesClient:
    """WebSocket client for StandX public trades data."""

    WS_URL = "wss://perps.standx.com/ws-stream/v1"

    def __init__(self, symbol: str = "BTC-USD", max_trades: int = 100):
        self.symbol = symbol
        self.max_trades = max_trades
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.trades: deque = deque(maxlen=max_trades)
        self.running = False
        self.trade_count = 0

    async def connect(self):
        """Establish WebSocket connection."""
        logger.info(f"Connecting to {self.WS_URL}")
        self.ws = await websockets.connect(
            self.WS_URL,
            ping_interval=20,
            ping_timeout=10
        )
        self.running = True
        logger.info("Connected successfully")

    async def subscribe(self):
        """Subscribe to public_trade channel."""
        subscribe_msg = {
            "subscribe": {
                "channel": "public_trade",
                "symbol": self.symbol
            }
        }
        await self.ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to public_trade for {self.symbol}")

    async def unsubscribe(self):
        """Unsubscribe from public_trade channel."""
        unsubscribe_msg = {
            "unsubscribe": {
                "channel": "public_trade",
                "symbol": self.symbol
            }
        }
        await self.ws.send(json.dumps(unsubscribe_msg))
        logger.info(f"Unsubscribed from public_trade for {self.symbol}")

    def process_trade(self, data: Dict) -> Dict:
        """Process incoming trade data."""
        trade = {
            "symbol": data.get("symbol", self.symbol),
            "price": data.get("price"),
            "qty": data.get("qty"),
            "quote_qty": data.get("quote_qty"),
            "side": "BUY" if data.get("is_buyer_taker") else "SELL",
            "is_buyer_taker": data.get("is_buyer_taker"),
            "timestamp": data.get("time", datetime.utcnow().isoformat()),
            "received_at": datetime.utcnow().isoformat()
        }

        self.trades.append(trade)
        self.trade_count += 1

        return trade

    def get_trades(self, limit: Optional[int] = None) -> List[Dict]:
        """Return recent trades."""
        trades_list = list(self.trades)
        if limit:
            return trades_list[-limit:]
        return trades_list

    def get_last_trade(self) -> Optional[Dict]:
        """Return the most recent trade."""
        if self.trades:
            return self.trades[-1]
        return None

    def get_last_price(self) -> Optional[float]:
        """Return the last traded price."""
        last_trade = self.get_last_trade()
        if last_trade:
            return float(last_trade["price"])
        return None

    def get_volume(self, seconds: int = 60) -> float:
        """Calculate volume over last N seconds."""
        now = datetime.utcnow()
        volume = 0.0
        for trade in self.trades:
            try:
                trade_time = datetime.fromisoformat(trade["timestamp"].replace("Z", "+00:00"))
                # Make now offset-aware if trade_time is offset-aware
                if trade_time.tzinfo is not None:
                    from datetime import timezone
                    now_aware = datetime.now(timezone.utc)
                    diff = (now_aware - trade_time).total_seconds()
                else:
                    diff = (now - trade_time).total_seconds()

                if diff <= seconds:
                    volume += float(trade["qty"])
            except Exception:
                continue
        return volume

    def get_buy_sell_ratio(self, last_n: int = 50) -> Dict:
        """Calculate buy/sell ratio for last N trades."""
        recent_trades = self.get_trades(last_n)
        buys = sum(1 for t in recent_trades if t["is_buyer_taker"])
        sells = len(recent_trades) - buys

        return {
            "buys": buys,
            "sells": sells,
            "ratio": buys / sells if sells > 0 else float('inf'),
            "buy_pct": (buys / len(recent_trades) * 100) if recent_trades else 0
        }

    def print_trade(self, trade: Dict):
        """Pretty print a single trade."""
        side_color = "\033[92m" if trade["side"] == "BUY" else "\033[91m"
        reset = "\033[0m"

        print(
            f"{trade['timestamp'][:19]} | "
            f"{side_color}{trade['side']:4}{reset} | "
            f"Price: {trade['price']:>12} | "
            f"Qty: {trade['qty']:>10} | "
            f"Value: {trade['quote_qty']:>12}"
        )

    def print_summary(self):
        """Print trade summary statistics."""
        if not self.trades:
            print("No trades received yet")
            return

        last_trade = self.get_last_trade()
        ratio = self.get_buy_sell_ratio()
        volume_1m = self.get_volume(60)

        print("\n" + "=" * 60)
        print(f"TRADES SUMMARY: {self.symbol}")
        print("=" * 60)
        print(f"Total trades received: {self.trade_count}")
        print(f"Last price: {last_trade['price']}")
        print(f"Last side: {last_trade['side']}")
        print(f"Volume (1m): {volume_1m:.4f}")
        print(f"Buy/Sell ratio (last 50): {ratio['buys']}/{ratio['sells']} ({ratio['buy_pct']:.1f}% buys)")
        print("=" * 60)

    async def listen(self, callback: Optional[Callable] = None):
        """Listen for trade updates."""
        try:
            async for message in self.ws:
                data = json.loads(message)

                # Check if it's a public_trade message
                if data.get("channel") == "public_trade":
                    trade_data = data.get("data", {})
                    trade = self.process_trade(trade_data)

                    # Call custom callback if provided
                    if callback:
                        await callback(trade)
                    else:
                        self.print_trade(trade)

                        # Print summary every 10 trades
                        if self.trade_count % 10 == 0:
                            self.print_summary()

                elif "error" in data:
                    logger.error(f"Error received: {data}")

        except websockets.ConnectionClosed as e:
            logger.warning(f"Connection closed: {e}")
            self.running = False
        except Exception as e:
            logger.error(f"Error in listener: {e}")
            self.running = False

    async def run(self, callback: Optional[Callable] = None):
        """Main run loop with reconnection logic."""
        while True:
            try:
                await self.connect()
                await self.subscribe()
                await self.listen(callback)
            except Exception as e:
                logger.error(f"Connection error: {e}")

            if not self.running:
                break

            logger.info("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

    async def close(self):
        """Close the WebSocket connection."""
        self.running = False
        if self.ws:
            await self.unsubscribe()
            await self.ws.close()
            logger.info("Connection closed")


async def main():
    """Example usage."""
    client = TradesClient(symbol="BTC-USD", max_trades=100)

    try:
        await client.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
