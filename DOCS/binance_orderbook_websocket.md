# Binance Spot Orderbook WebSocket API

This document covers how to stream BTC (and other) orderbook data via Binance WebSocket API.

## WebSocket Endpoints

| Endpoint | Description |
|----------|-------------|
| `wss://stream.binance.com:9443` | Primary endpoint |
| `wss://stream.binance.com:443` | Alternative port |
| `wss://data-stream.binance.vision` | Market data only (no user data) |

## Stream Types

### 1. Partial Book Depth Streams

Top bids and asks at specified depth levels. Pushed every second or 100ms.

**Stream Names:**
- `<symbol>@depth<levels>` - 1000ms updates
- `<symbol>@depth<levels>@100ms` - 100ms updates

**Valid levels:** 5, 10, or 20

**Examples for BTCUSDT:**
```
btcusdt@depth5        # Top 5 levels, 1s updates
btcusdt@depth10       # Top 10 levels, 1s updates
btcusdt@depth20@100ms # Top 20 levels, 100ms updates
```

**Payload:**
```json
{
  "lastUpdateId": 160,
  "bids": [
    ["42150.00", "1.234"]   // [price, quantity]
  ],
  "asks": [
    ["42151.00", "0.567"]
  ]
}
```

### 2. Diff. Depth Stream (Incremental Updates)

Order book price and quantity depth updates. Use this to maintain a local orderbook.

**Stream Names:**
- `<symbol>@depth` - 1000ms updates
- `<symbol>@depth@100ms` - 100ms updates

**Examples for BTCUSDT:**
```
btcusdt@depth         # 1s updates
btcusdt@depth@100ms   # 100ms updates
```

**Payload:**
```json
{
  "e": "depthUpdate",     // Event type
  "E": 1672515782136,     // Event time (ms)
  "s": "BTCUSDT",         // Symbol
  "U": 157,               // First update ID in event
  "u": 160,               // Final update ID in event
  "b": [                  // Bids to update
    ["42150.00", "1.234"] // [price, quantity]
  ],
  "a": [                  // Asks to update
    ["42151.00", "0.567"]
  ]
}
```

## Subscribing to Streams

### Method 1: URL Path
```
wss://stream.binance.com:9443/ws/btcusdt@depth@100ms
```

### Method 2: Combined Streams
```
wss://stream.binance.com:9443/stream?streams=btcusdt@depth@100ms/ethusdt@depth@100ms
```

### Method 3: Subscribe Message
```json
{
  "method": "SUBSCRIBE",
  "params": [
    "btcusdt@depth@100ms",
    "btcusdt@depth5"
  ],
  "id": 1
}
```

**Unsubscribe:**
```json
{
  "method": "UNSUBSCRIBE",
  "params": ["btcusdt@depth@100ms"],
  "id": 2
}
```

## Maintaining a Local Orderbook

To maintain an accurate local orderbook, follow these steps:

### Step 1: Connect and Buffer
1. Open WebSocket to `wss://stream.binance.com:9443/ws/btcusdt@depth`
2. Buffer all incoming events

### Step 2: Get Initial Snapshot
3. Get depth snapshot via REST API:
   ```
   GET https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000
   ```

   Response:
   ```json
   {
     "lastUpdateId": 1027024,
     "bids": [["42150.00", "1.234"], ...],
     "asks": [["42151.00", "0.567"], ...]
   }
   ```

### Step 3: Synchronize
4. Discard buffered events where `u` <= snapshot's `lastUpdateId`
5. First valid event must satisfy: `U` <= `lastUpdateId` AND `u` >= `lastUpdateId`
6. If condition not met, get a new snapshot

### Step 4: Apply Updates
7. For each event after synchronization:
   - Update the price level with the new quantity
   - If quantity is `0`, **remove** the price level
   - Quantities are absolute (replace, not add)

### Step 5: Validate Continuity
8. Each event's first update ID (`U`) should equal previous event's `u` + 1
9. If gap detected, re-sync from step 2

## Connection Limits

| Limit | Value |
|-------|-------|
| Max streams per connection | 1024 |
| Incoming messages rate | 5 per second |
| Connection lifetime | 24 hours max |
| Connection attempts | 300 per 5 min per IP |

## Keep-Alive

- Server sends **ping** every 20 seconds
- Must respond with **pong** within 60 seconds
- Failure to respond disconnects the connection

## Python Examples

### Using python-binance (Async)

```python
import asyncio
from binance import AsyncClient, BinanceSocketManager

async def main():
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)

    # Depth stream for BTCUSDT
    ds = bm.depth_socket('BTCUSDT')

    async with ds as dcm:
        while True:
            msg = await dcm.recv()
            print(f"Update ID: {msg['u']}")
            print(f"Bids: {msg['b'][:3]}")  # Top 3 bid updates
            print(f"Asks: {msg['a'][:3]}")  # Top 3 ask updates

    await client.close_connection()

if __name__ == "__main__":
    asyncio.run(main())
```

### Using python-binance (Threaded)

```python
from binance import ThreadedWebsocketManager

def handle_message(msg):
    if msg['e'] == 'depthUpdate':
        print(f"Symbol: {msg['s']}")
        print(f"Best bid update: {msg['b'][0] if msg['b'] else 'none'}")
        print(f"Best ask update: {msg['a'][0] if msg['a'] else 'none'}")

twm = ThreadedWebsocketManager()
twm.start()

# Start depth stream
twm.start_depth_socket(callback=handle_message, symbol='BTCUSDT')

twm.join()
```

### Raw WebSocket (No Library)

```python
import asyncio
import websockets
import json

async def stream_orderbook():
    uri = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms"

    async with websockets.connect(uri) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            print(f"Event time: {data['E']}")
            print(f"Update ID: {data['U']} -> {data['u']}")
            print(f"Bid updates: {len(data['b'])}")
            print(f"Ask updates: {len(data['a'])}")

asyncio.run(stream_orderbook())
```

### Full Local Orderbook Manager

```python
import asyncio
import aiohttp
import websockets
import json
from collections import OrderedDict

class BinanceOrderbook:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.bids = OrderedDict()  # price -> qty (descending)
        self.asks = OrderedDict()  # price -> qty (ascending)
        self.last_update_id = 0
        self.buffer = []
        self.synced = False

    async def get_snapshot(self):
        url = f"https://api.binance.com/api/v3/depth?symbol={self.symbol}&limit=1000"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                return await resp.json()

    def apply_snapshot(self, snapshot):
        self.last_update_id = snapshot['lastUpdateId']
        self.bids.clear()
        self.asks.clear()

        for price, qty in snapshot['bids']:
            self.bids[price] = qty
        for price, qty in snapshot['asks']:
            self.asks[price] = qty

    def apply_update(self, event):
        for price, qty in event['b']:
            if float(qty) == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for price, qty in event['a']:
            if float(qty) == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

        self.last_update_id = event['u']

    async def run(self):
        uri = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@depth@100ms"

        async with websockets.connect(uri) as ws:
            # Buffer initial events
            while not self.synced:
                msg = json.loads(await ws.recv())
                self.buffer.append(msg)

                if len(self.buffer) >= 5:  # Get snapshot after buffering
                    snapshot = await self.get_snapshot()
                    self.apply_snapshot(snapshot)

                    # Apply buffered events
                    for event in self.buffer:
                        if event['u'] <= self.last_update_id:
                            continue
                        if event['U'] <= self.last_update_id + 1:
                            self.apply_update(event)
                            self.synced = True

                    self.buffer.clear()

            # Process live updates
            while True:
                msg = json.loads(await ws.recv())
                self.apply_update(msg)

                # Print top of book
                best_bid = next(iter(self.bids.items()), None)
                best_ask = next(iter(self.asks.items()), None)
                print(f"Best bid: {best_bid}, Best ask: {best_ask}")

# Usage
ob = BinanceOrderbook('BTCUSDT')
asyncio.run(ob.run())
```

## REST API Reference

### Get Depth Snapshot

```
GET /api/v3/depth
```

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| symbol | STRING | Yes | e.g., BTCUSDT |
| limit | INT | No | Default 100, max 5000 |

**Example:**
```bash
curl "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
```

## Sources

- [Binance WebSocket Streams Documentation](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [Binance Market Data Endpoints](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints)
- [How to Manage a Local Order Book Correctly](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)
- [python-binance Documentation](https://python-binance.readthedocs.io/en/latest/websockets.html)
- [GitHub: binance-spot-api-docs](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md)
