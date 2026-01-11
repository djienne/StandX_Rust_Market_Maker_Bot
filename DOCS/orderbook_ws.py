"""
StandX WebSocket Orderbook Client
Subscribes to depth_book channel and maintains orderbook with 10+ levels
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
import websockets

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderbookClient:
    """WebSocket client for StandX orderbook data."""

    WS_URL = "wss://perps.standx.com/ws-stream/v1"

    def __init__(self, symbol: str = "BTC-USD", levels: int = 10):
        self.symbol = symbol
        self.levels = levels
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.orderbook: Dict = {
            "symbol": symbol,
            "bids": [],  # [[price, qty], ...]
            "asks": [],  # [[price, qty], ...]
            "sequence": 0,
            "timestamp": None
        }
        self.running = False

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
        """Subscribe to depth_book channel."""
        subscribe_msg = {
            "subscribe": {
                "channel": "depth_book",
                "symbol": self.symbol
            }
        }
        await self.ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to depth_book for {self.symbol}")

    async def unsubscribe(self):
        """Unsubscribe from depth_book channel."""
        unsubscribe_msg = {
            "unsubscribe": {
                "channel": "depth_book",
                "symbol": self.symbol
            }
        }
        await self.ws.send(json.dumps(unsubscribe_msg))
        logger.info(f"Unsubscribed from depth_book for {self.symbol}")

    def process_orderbook(self, data: Dict):
        """Process orderbook update and maintain top N levels."""
        self.orderbook["symbol"] = data.get("symbol", self.symbol)
        self.orderbook["sequence"] = data.get("sequence", 0)
        self.orderbook["timestamp"] = data.get("time", datetime.utcnow().isoformat())

        # Get asks and bids, limit to requested levels
        asks = data.get("asks", [])
        bids = data.get("bids", [])

        # Sort asks ascending (best ask first)
        asks_sorted = sorted(asks, key=lambda x: float(x[0]))[:self.levels]

        # Sort bids descending (best bid first)
        bids_sorted = sorted(bids, key=lambda x: float(x[0]), reverse=True)[:self.levels]

        self.orderbook["asks"] = asks_sorted
        self.orderbook["bids"] = bids_sorted

    def get_orderbook(self) -> Dict:
        """Return current orderbook state."""
        return self.orderbook.copy()

    def get_best_bid(self) -> Optional[List]:
        """Return best bid [price, qty]."""
        if self.orderbook["bids"]:
            return self.orderbook["bids"][0]
        return None

    def get_best_ask(self) -> Optional[List]:
        """Return best ask [price, qty]."""
        if self.orderbook["asks"]:
            return self.orderbook["asks"][0]
        return None

    def get_spread(self) -> Optional[float]:
        """Calculate bid-ask spread."""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid and best_ask:
            return float(best_ask[0]) - float(best_bid[0])
        return None

    def get_mid_price(self) -> Optional[float]:
        """Calculate mid price."""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid and best_ask:
            return (float(best_ask[0]) + float(best_bid[0])) / 2
        return None

    def print_orderbook(self):
        """Pretty print the orderbook."""
        print("\n" + "=" * 60)
        print(f"ORDERBOOK: {self.orderbook['symbol']}")
        print(f"Timestamp: {self.orderbook['timestamp']}")
        print(f"Sequence: {self.orderbook['sequence']}")
        print("=" * 60)

        # Print asks in reverse (highest first for display)
        print(f"{'ASKS':<30}")
        print(f"{'Price':<15} {'Quantity':<15}")
        print("-" * 30)
        for ask in reversed(self.orderbook["asks"]):
            print(f"{ask[0]:<15} {ask[1]:<15}")

        # Spread
        spread = self.get_spread()
        mid = self.get_mid_price()
        print("-" * 30)
        print(f"SPREAD: {spread:.2f} | MID: {mid:.2f}" if spread and mid else "")
        print("-" * 30)

        # Print bids
        print(f"{'BIDS':<30}")
        print(f"{'Price':<15} {'Quantity':<15}")
        print("-" * 30)
        for bid in self.orderbook["bids"]:
            print(f"{bid[0]:<15} {bid[1]:<15}")

        print("=" * 60)

    async def listen(self, callback=None):
        """Listen for orderbook updates."""
        try:
            async for message in self.ws:
                data = json.loads(message)

                # Check if it's a depth_book message
                if data.get("channel") == "depth_book":
                    book_data = data.get("data", {})
                    self.process_orderbook(book_data)

                    # Call custom callback if provided
                    if callback:
                        await callback(self.orderbook)
                    else:
                        self.print_orderbook()

                elif "error" in data:
                    logger.error(f"Error received: {data}")

        except websockets.ConnectionClosed as e:
            logger.warning(f"Connection closed: {e}")
            self.running = False
        except Exception as e:
            logger.error(f"Error in listener: {e}")
            self.running = False

    async def run(self, callback=None):
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
    client = OrderbookClient(symbol="BTC-USD", levels=10)

    try:
        await client.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
