# StandX WebSocket API

## Connection Details

| Stream Type | Endpoint |
|-------------|----------|
| **Market Stream** | `wss://perps.standx.com/ws-stream/v1` |
| **Order Response Stream** | `wss://perps.standx.com/ws-api/v1` |

---

## Connection Management

### Ping/Pong Mechanism

- Server initiates WebSocket Ping frames every **10 seconds**
- Clients must respond with Pong frames
- Connection terminates after **5 minutes** without Pong response
- Most modern browsers/libraries handle this automatically

**Timeout Error:**
```json
{
  "code": 408,
  "message": "disconnecting due to not receive Pong within 5 minute period"
}
```

---

## Available Channels

### Public Channels (No Auth Required)

| Channel | Description |
|---------|-------------|
| `price` | Symbol price data |
| `depth_book` | Order book depth |
| `public_trade` | Public trade information |

### Authenticated Channels (User-Level)

| Channel | Description |
|---------|-------------|
| `order` | User orders |
| `position` | User positions |
| `balance` | User balance |
| `trade` | User trades |

---

# Market Stream

**Endpoint:** `wss://perps.standx.com/ws-stream/v1`

## Subscribing to Depth Book

### Request

```json
{
  "subscribe": {
    "channel": "depth_book",
    "symbol": "BTC-USD"
  }
}
```

### Response

```json
{
  "seq": 1,
  "channel": "depth_book",
  "data": {
    "symbol": "BTC-USD",
    "asks": [
      ["121895.81", "0.843"],
      ["121896.11", "0.96"]
    ],
    "bids": [
      ["121884.01", "0.001"],
      ["121884.31", "0.001"]
    ],
    "sequence": 123456,
    "time": "2025-08-11T03:44:40.922233Z"
  }
}
```

---

## Subscribing to Symbol Price

### Request

```json
{
  "subscribe": {
    "channel": "price",
    "symbol": "BTC-USD"
  }
}
```

### Response

```json
{
  "seq": 1,
  "channel": "price",
  "data": {
    "symbol": "BTC-USD",
    "index_price": "121601.158461",
    "last_price": "121599.94",
    "mark_price": "121602.43",
    "mid_price": "121599.99",
    "spread": ["121599.94", "121600.04"],
    "time": "2025-08-11T03:44:40.922233Z"
  }
}
```

---

## Subscribing to Public Trades

### Request

```json
{
  "subscribe": {
    "channel": "public_trade",
    "symbol": "BTC-USD"
  }
}
```

### Response

```json
{
  "seq": 1,
  "channel": "public_trade",
  "data": {
    "symbol": "BTC-USD",
    "price": "121720.18",
    "qty": "0.01",
    "quote_qty": "1217.2018",
    "is_buyer_taker": true,
    "time": "2025-08-11T03:48:47.086505Z"
  }
}
```

---

## Authenticated Streams

### Authentication Request

```json
{
  "auth": {
    "token": "<jwt>",
    "streams": [
      { "channel": "order" },
      { "channel": "position" },
      { "channel": "balance" },
      { "channel": "trade" }
    ]
  }
}
```

### Authentication Response

```json
{
  "seq": 1,
  "channel": "auth",
  "data": {
    "code": 200,
    "msg": "success"
  }
}
```

---

## User Order Updates

After authentication, order updates are pushed automatically:

```json
{
  "seq": 2,
  "channel": "order",
  "data": {
    "id": 1820682,
    "cl_ord_id": "01K2BK4ZKQE0C308SRD39P8N9Z",
    "symbol": "BTC-USD",
    "side": "sell",
    "status": "filled",
    "price": "121900.00",
    "qty": "0.060",
    "fill_qty": "0.060",
    "fill_avg_price": "121900.00",
    "leverage": "10",
    "order_type": "limit",
    "time_in_force": "gtc",
    "reduce_only": false,
    "position_id": 15,
    "created_at": "2025-08-11T03:35:25.559151Z",
    "updated_at": "2025-08-11T03:36:19.352620Z"
  }
}
```

---

## User Position Updates

```json
{
  "seq": 3,
  "channel": "position",
  "data": {
    "id": 15,
    "symbol": "BTC-USD",
    "status": "open",
    "qty": "0.940",
    "entry_price": "121737.96",
    "leverage": "10",
    "margin_mode": "isolated",
    "realized_pnl": "31.61532",
    "upnl": "-21.53540",
    "time": "2025-08-11T03:41:40.922818Z"
  }
}
```

---

## User Balance Updates

```json
{
  "seq": 4,
  "channel": "balance",
  "data": {
    "token": "DUSD",
    "free": "1085746.571",
    "locked": "0.000000000",
    "status": "active",
    "time": "2025-08-11T03:41:40.922818Z"
  }
}
```

---

## User Trade Updates

```json
{
  "seq": 5,
  "channel": "trade",
  "data": {
    "id": 409870,
    "order_id": 1820682,
    "symbol": "BTC-USD",
    "price": "121900",
    "qty": "0.01",
    "side": "sell",
    "fee_asset": "DUSD",
    "fee_qty": "0.121900",
    "value": "1219.00",
    "pnl": "1.62040",
    "time": "2025-08-11T03:36:19.352620Z"
  }
}
```

---

# Order Response Stream

**Endpoint:** `wss://perps.standx.com/ws-api/v1`

This stream allows you to submit orders and receive responses via WebSocket.

## Request Structure

All requests follow this format:

```json
{
  "session_id": "<uuid>",
  "request_id": "<uuid>",
  "method": "<method>",
  "header": {
    "x-request-id": "<uuid>",
    "x-request-timestamp": "<timestamp_ms>",
    "x-request-signature": "<base64_signature>"
  },
  "params": "<json_string>"
}
```

---

## Authentication

### Method: `auth:login`

### Request

```json
{
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "request_id": "550e8400-e29b-41d4-a716-446655440001",
  "method": "auth:login",
  "header": {},
  "params": "{\"token\":\"<jwt>\"}"
}
```

### Response

```json
{
  "code": 0,
  "message": "success",
  "request_id": "550e8400-e29b-41d4-a716-446655440001"
}
```

---

## Create Order

### Method: `order:new`

### Request

```json
{
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "request_id": "550e8400-e29b-41d4-a716-446655440002",
  "method": "order:new",
  "header": {
    "x-request-id": "550e8400-e29b-41d4-a716-446655440002",
    "x-request-timestamp": "1620000000000",
    "x-request-signature": "base64EncodedSignature=="
  },
  "params": "{\"symbol\":\"BTC-USD\",\"side\":\"buy\",\"order_type\":\"limit\",\"qty\":\"0.1\",\"price\":\"50000\",\"time_in_force\":\"gtc\",\"reduce_only\":false}"
}
```

### Success Response

```json
{
  "code": 0,
  "message": "success",
  "request_id": "550e8400-e29b-41d4-a716-446655440002"
}
```

### Rejection Response

```json
{
  "code": 400,
  "message": "alo order rejected",
  "request_id": "550e8400-e29b-41d4-a716-446655440002"
}
```

---

## Cancel Order

### Method: `order:cancel`

### Request

```json
{
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "request_id": "550e8400-e29b-41d4-a716-446655440003",
  "method": "order:cancel",
  "header": {
    "x-request-id": "550e8400-e29b-41d4-a716-446655440003",
    "x-request-timestamp": "1620000000000",
    "x-request-signature": "base64EncodedSignature=="
  },
  "params": "{\"order_id\":2424844}"
}
```

### Response

```json
{
  "code": 0,
  "message": "success",
  "request_id": "550e8400-e29b-41d4-a716-446655440003"
}
```

---

# JavaScript Example

```javascript
// Market Stream - Subscribe to price updates
const marketWs = new WebSocket('wss://perps.standx.com/ws-stream/v1');

marketWs.onopen = () => {
  console.log('Market stream connected');

  // Subscribe to price channel
  marketWs.send(JSON.stringify({
    subscribe: { channel: 'price', symbol: 'BTC-USD' }
  }));

  // Subscribe to depth book
  marketWs.send(JSON.stringify({
    subscribe: { channel: 'depth_book', symbol: 'BTC-USD' }
  }));
};

marketWs.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Market update:', data);
};

// Order Stream - Authenticated trading
const orderWs = new WebSocket('wss://perps.standx.com/ws-api/v1');

orderWs.onopen = () => {
  console.log('Order stream connected');

  // Authenticate
  orderWs.send(JSON.stringify({
    session_id: 'your-session-id',
    request_id: 'auth-request-id',
    method: 'auth:login',
    header: {},
    params: JSON.stringify({ token: 'your-jwt-token' })
  }));
};

orderWs.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Order update:', data);
};
```

---

# Python Example

```python
import asyncio
import websockets
import json

async def market_stream():
    uri = "wss://perps.standx.com/ws-stream/v1"

    async with websockets.connect(uri) as ws:
        # Subscribe to price channel
        await ws.send(json.dumps({
            "subscribe": {"channel": "price", "symbol": "BTC-USD"}
        }))

        # Listen for updates
        async for message in ws:
            data = json.loads(message)
            print(f"Price update: {data}")

async def order_stream(jwt_token):
    uri = "wss://perps.standx.com/ws-api/v1"

    async with websockets.connect(uri) as ws:
        # Authenticate
        await ws.send(json.dumps({
            "session_id": "your-session-id",
            "request_id": "auth-request-id",
            "method": "auth:login",
            "header": {},
            "params": json.dumps({"token": jwt_token})
        }))

        # Listen for order updates
        async for message in ws:
            data = json.loads(message)
            print(f"Order update: {data}")

# Run market stream
asyncio.run(market_stream())
```

---

# Unsubscribing

To unsubscribe from a channel:

```json
{
  "unsubscribe": {
    "channel": "price",
    "symbol": "BTC-USD"
  }
}
```

---

# Error Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 400 | Bad Request / Order Rejected |
| 401 | Unauthorized |
| 408 | Connection Timeout |
| 500 | Internal Server Error |
