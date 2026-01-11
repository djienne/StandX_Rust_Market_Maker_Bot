"""
StandX WebSocket Balance Client
Subscribes to authenticated balance channel for real-time balance updates
Requires JWT authentication
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Callable, Dict, Optional
import websockets

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BalanceClient:
    """WebSocket client for StandX balance data (authenticated)."""

    WS_STREAM_URL = "wss://perps.standx.com/ws-stream/v1"

    def __init__(self, jwt_token: str):
        """
        Initialize balance client.

        Args:
            jwt_token: JWT authentication token from StandX auth flow
        """
        self.jwt_token = jwt_token
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.balance: Dict = {
            "token": None,
            "free": "0",
            "locked": "0",
            "status": None,
            "timestamp": None
        }
        self.running = False
        self.authenticated = False

    async def connect(self):
        """Establish WebSocket connection."""
        logger.info(f"Connecting to {self.WS_STREAM_URL}")
        self.ws = await websockets.connect(
            self.WS_STREAM_URL,
            ping_interval=20,
            ping_timeout=10
        )
        self.running = True
        logger.info("Connected successfully")

    async def authenticate(self):
        """Authenticate and subscribe to balance channel."""
        auth_msg = {
            "auth": {
                "token": self.jwt_token,
                "streams": [
                    {"channel": "balance"}
                ]
            }
        }
        await self.ws.send(json.dumps(auth_msg))
        logger.info("Authentication request sent, subscribing to balance channel")

    async def subscribe_additional_channels(self, channels: list):
        """Subscribe to additional authenticated channels."""
        auth_msg = {
            "auth": {
                "token": self.jwt_token,
                "streams": [{"channel": ch} for ch in channels]
            }
        }
        await self.ws.send(json.dumps(auth_msg))
        logger.info(f"Subscribed to additional channels: {channels}")

    def process_balance(self, data: Dict):
        """Process incoming balance data."""
        self.balance = {
            "token": data.get("token", "DUSD"),
            "free": data.get("free", "0"),
            "locked": data.get("locked", "0"),
            "status": data.get("status"),
            "timestamp": data.get("time", datetime.utcnow().isoformat()),
            "received_at": datetime.utcnow().isoformat()
        }

    def get_balance(self) -> Dict:
        """Return current balance state."""
        return self.balance.copy()

    def get_free_balance(self) -> float:
        """Return free balance as float."""
        return float(self.balance.get("free", 0))

    def get_locked_balance(self) -> float:
        """Return locked balance as float."""
        return float(self.balance.get("locked", 0))

    def get_total_balance(self) -> float:
        """Return total balance (free + locked)."""
        return self.get_free_balance() + self.get_locked_balance()

    def print_balance(self):
        """Pretty print the current balance."""
        print("\n" + "=" * 50)
        print("BALANCE UPDATE")
        print("=" * 50)
        print(f"Token:     {self.balance['token']}")
        print(f"Free:      {self.balance['free']}")
        print(f"Locked:    {self.balance['locked']}")
        print(f"Total:     {self.get_total_balance():.6f}")
        print(f"Status:    {self.balance['status']}")
        print(f"Timestamp: {self.balance['timestamp']}")
        print("=" * 50)

    async def listen(self, callback: Optional[Callable] = None):
        """Listen for balance updates."""
        try:
            async for message in self.ws:
                data = json.loads(message)
                channel = data.get("channel")

                # Handle auth response
                if channel == "auth":
                    auth_data = data.get("data", {})
                    if auth_data.get("code") == 200:
                        self.authenticated = True
                        logger.info("Authentication successful")
                    else:
                        logger.error(f"Authentication failed: {auth_data}")
                        self.running = False
                        return

                # Handle balance updates
                elif channel == "balance":
                    balance_data = data.get("data", {})
                    self.process_balance(balance_data)

                    if callback:
                        await callback(self.balance)
                    else:
                        self.print_balance()

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
                await self.authenticate()
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
            await self.ws.close()
            logger.info("Connection closed")


class FullAccountClient(BalanceClient):
    """
    Extended client that subscribes to all account-related channels:
    - balance
    - order
    - position
    - trade
    """

    def __init__(self, jwt_token: str):
        super().__init__(jwt_token)
        self.orders: Dict = {}
        self.positions: Dict = {}
        self.trades: list = []

    async def authenticate(self):
        """Authenticate and subscribe to all account channels."""
        auth_msg = {
            "auth": {
                "token": self.jwt_token,
                "streams": [
                    {"channel": "balance"},
                    {"channel": "order"},
                    {"channel": "position"},
                    {"channel": "trade"}
                ]
            }
        }
        await self.ws.send(json.dumps(auth_msg))
        logger.info("Authentication request sent, subscribing to all account channels")

    def process_order(self, data: Dict):
        """Process incoming order data."""
        order_id = data.get("id")
        if order_id:
            self.orders[order_id] = data
            logger.info(f"Order update: {data.get('status')} - ID: {order_id}")

    def process_position(self, data: Dict):
        """Process incoming position data."""
        symbol = data.get("symbol")
        if symbol:
            self.positions[symbol] = data
            logger.info(f"Position update: {symbol} - Qty: {data.get('qty')}")

    def process_trade(self, data: Dict):
        """Process incoming trade data."""
        self.trades.append(data)
        logger.info(f"Trade: {data.get('side')} {data.get('qty')} @ {data.get('price')}")

    async def listen(self, callback: Optional[Callable] = None):
        """Listen for all account updates."""
        try:
            async for message in self.ws:
                data = json.loads(message)
                channel = data.get("channel")

                # Handle auth response
                if channel == "auth":
                    auth_data = data.get("data", {})
                    if auth_data.get("code") == 200:
                        self.authenticated = True
                        logger.info("Authentication successful")
                    else:
                        logger.error(f"Authentication failed: {auth_data}")
                        self.running = False
                        return

                # Handle different channel updates
                elif channel == "balance":
                    self.process_balance(data.get("data", {}))
                    if callback:
                        await callback("balance", self.balance)
                    else:
                        self.print_balance()

                elif channel == "order":
                    self.process_order(data.get("data", {}))
                    if callback:
                        await callback("order", data.get("data", {}))

                elif channel == "position":
                    self.process_position(data.get("data", {}))
                    if callback:
                        await callback("position", data.get("data", {}))

                elif channel == "trade":
                    self.process_trade(data.get("data", {}))
                    if callback:
                        await callback("trade", data.get("data", {}))

                elif "error" in data:
                    logger.error(f"Error received: {data}")

        except websockets.ConnectionClosed as e:
            logger.warning(f"Connection closed: {e}")
            self.running = False
        except Exception as e:
            logger.error(f"Error in listener: {e}")
            self.running = False


async def main():
    """
    Example usage.

    To use this client, you need a valid JWT token from StandX authentication.
    See authentication.md in DOCS folder for the auth flow.
    """
    # Get JWT token from environment variable or config
    jwt_token = os.environ.get("STANDX_JWT_TOKEN")

    if not jwt_token:
        print("=" * 60)
        print("STANDX BALANCE WEBSOCKET CLIENT")
        print("=" * 60)
        print("\nThis client requires authentication.")
        print("\nTo use:")
        print("1. Complete the StandX auth flow to get a JWT token")
        print("2. Set the STANDX_JWT_TOKEN environment variable:")
        print("   export STANDX_JWT_TOKEN='your_jwt_token_here'")
        print("\nOr modify this script to pass the token directly:")
        print("   client = BalanceClient(jwt_token='your_token')")
        print("\nSee DOCS/authentication.md for the complete auth flow.")
        print("=" * 60)
        return

    # Create client and run
    client = BalanceClient(jwt_token=jwt_token)

    try:
        await client.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await client.close()


async def main_full_account():
    """Example usage for full account monitoring."""
    jwt_token = os.environ.get("STANDX_JWT_TOKEN")

    if not jwt_token:
        print("Please set STANDX_JWT_TOKEN environment variable")
        return

    client = FullAccountClient(jwt_token=jwt_token)

    async def on_update(channel: str, data: Dict):
        """Custom callback for all updates."""
        print(f"\n[{channel.upper()}] Update received:")
        print(json.dumps(data, indent=2))

    try:
        await client.run(callback=on_update)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await client.close()


if __name__ == "__main__":
    # Run balance-only client
    asyncio.run(main())

    # Or run full account client (uncomment below)
    # asyncio.run(main_full_account())
