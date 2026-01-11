# StandX API Reference

## Enums & Constants

### Trading Pairs (Symbols)

| Symbol | Description |
|--------|-------------|
| `BTC-USD` | Bitcoin / USD perpetual |

---

### Margin Modes

| Value | Description |
|-------|-------------|
| `cross` | Cross margin - shared margin across positions |
| `isolated` | Isolated margin - dedicated margin per position |

---

### Tokens

| Token | Description |
|-------|-------------|
| `DUSD` | StandX USD stablecoin (quote asset) |

---

### Order Side

| Value | Description |
|-------|-------------|
| `buy` | Long / Buy order |
| `sell` | Short / Sell order |

---

### Order Type

| Value | Description |
|-------|-------------|
| `limit` | Limit order - executes at specified price or better |
| `market` | Market order - executes immediately at best available price |

---

### Order Status

| Value | Description |
|-------|-------------|
| `open` | Order is active and waiting to be filled |
| `canceled` | Order was canceled by user |
| `filled` | Order has been completely filled |
| `rejected` | Order was rejected by the system |
| `untriggered` | Conditional order waiting for trigger |

---

### Time In Force

| Value | Description |
|-------|-------------|
| `gtc` | **Good Til Canceled** - Order remains active until canceled |
| `ioc` | **Immediate Or Cancel** - Fill as much as possible immediately, cancel rest |
| `alo` | **Add Liquidity Only** - Order added to book without immediate execution; executes as resting order (maker only) |

---

### Kline Resolutions

| Value | Description |
|-------|-------------|
| `1T` | 1 tick |
| `3S` | 3 seconds |
| `1` | 1 minute |
| `5` | 5 minutes |
| `15` | 15 minutes |
| `60` | 1 hour |
| `1D` | 1 day |
| `1W` | 1 week |
| `1M` | 1 month |

---

## HTTP Status Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad Request - Invalid request parameters |
| 401 | Unauthorized - Authentication required or invalid token |
| 403 | Forbidden - Insufficient permissions |
| 404 | Not Found - Resource not found |
| 429 | Too Many Requests - Rate limit exceeded |
| 500 | Internal Server Error - Server error |

---

## API Response Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 400 | Bad Request / Order Rejected |
| 401 | Unauthorized |
| 408 | Connection Timeout (WebSocket) |
| 500 | Internal Server Error |

---

## Symbol Configuration

### BTC-USD Specifications

| Parameter | Value |
|-----------|-------|
| Base Asset | BTC |
| Quote Asset | DUSD |
| Base Decimals | 9 |
| Quote Decimals | 9 |
| Price Tick Decimals | 2 |
| Quantity Tick Decimals | 3 |
| Minimum Order Quantity | 0.001 |
| Maximum Order Quantity | 100 |
| Maximum Position Size | 1000 |
| Maximum Open Orders | 100 |
| Maximum Leverage | 20x |
| Default Leverage | 10x |
| Maker Fee | 0.01% (0.0001) |
| Taker Fee | 0.04% (0.0004) |
| Depth Ticks | 0.01, 0.1, 1 |
| Price Cap Ratio | 30% (0.3) |
| Price Floor Ratio | 30% (0.3) |

---

## Data Types

### Decimal Format

All price and quantity values should be sent as **JSON strings**, not floats:

```json
// Correct
{ "price": "50000.00", "qty": "0.1" }

// Incorrect
{ "price": 50000.00, "qty": 0.1 }
```

### Integer Format

Timestamps and IDs should be **JSON integers**:

```json
// Correct
{ "order_id": 1820682, "timestamp": 1620000000 }

// Incorrect
{ "order_id": "1820682", "timestamp": "1620000000" }
```

### Timestamp Formats

| Context | Format | Example |
|---------|--------|---------|
| Query parameters | ISO 8601 | `2025-08-11T03:35:25.559151Z` |
| Kline from/to | Unix seconds | `1754897028` |
| Funding rates | Unix milliseconds | `1620000000000` |
| Request headers | Unix milliseconds | `1620000000000` |

---

## Base URLs

| Service | URL |
|---------|-----|
| Authentication API | `https://api.standx.com` |
| Perps HTTP API | `https://perps.standx.com` |
| WebSocket Market Stream | `wss://perps.standx.com/ws-stream/v1` |
| WebSocket Order Stream | `wss://perps.standx.com/ws-api/v1` |
| Geo Service | `https://geo.standx.com` |
| Certificates | `https://api.standx.com/v1/offchain/certs` |

---

## Authentication Headers

### Standard Request

```
Authorization: Bearer <jwt_token>
```

### Body-Signed Request

```
Authorization: Bearer <jwt_token>
x-request-sign-version: v1
x-request-id: <uuid>
x-request-timestamp: <timestamp_ms>
x-request-signature: <base64_signature>
```

### Session Tracking

```
x-session-id: <custom_session_id>
```

---

## Signature Algorithm

| Component | Algorithm/Format |
|-----------|------------------|
| Key Generation | ed25519 |
| JWT Signing | ES256 |
| Public Key Encoding | Base58 |
| Signature Encoding | Base64 |

### Signature Message Format

```
{version},{request_id},{timestamp},{payload}
```

Example:
```
v1,550e8400-e29b-41d4-a716-446655440000,1620000000000,{"symbol":"BTC-USD"}
```

---

## WebSocket Channels

### Public Channels

| Channel | Description |
|---------|-------------|
| `price` | Real-time price updates |
| `depth_book` | Order book updates |
| `public_trade` | Public trade feed |

### Private Channels (Requires Auth)

| Channel | Description |
|---------|-------------|
| `order` | User order updates |
| `position` | User position updates |
| `balance` | User balance updates |
| `trade` | User trade updates |

---

## WebSocket Methods (Order Stream)

| Method | Description |
|--------|-------------|
| `auth:login` | Authenticate with JWT |
| `order:new` | Create new order |
| `order:cancel` | Cancel existing order |

---

## Position Fields

| Field | Description |
|-------|-------------|
| `entry_price` | Average entry price |
| `entry_value` | Notional value at entry |
| `mark_price` | Current mark price |
| `position_value` | Current notional value |
| `holding_margin` | Margin currently held |
| `initial_margin` | Initial margin requirement |
| `maint_margin` | Maintenance margin requirement |
| `mmr` | Maintenance margin ratio |
| `liq_price` | Liquidation price |
| `bankruptcy_price` | Bankruptcy price |
| `realized_pnl` | Realized profit/loss |
| `upnl` | Unrealized profit/loss |

---

## Balance Fields

| Field | Description |
|-------|-------------|
| `isolated_balance` | Total in isolated margin wallets |
| `isolated_upnl` | Unrealized PnL in isolated positions |
| `cross_balance` | Free balance in cross margin |
| `cross_margin` | Margin used in cross positions |
| `cross_upnl` | Unrealized PnL in cross positions |
| `locked` | Funds locked for pending orders |
| `cross_available` | Available for new cross orders |
| `balance` | Total account balance |
| `upnl` | Total unrealized PnL |
| `equity` | Total account equity |
| `pnl_freeze` | 24h realized PnL (display) |

---

## Fee Structure

| Fee Type | Rate |
|----------|------|
| Maker Fee | 0.01% (0.0001) |
| Taker Fee | 0.04% (0.0004) |

---

## Limits

| Limit | Value |
|-------|-------|
| Max Leverage | 20x |
| Max Order Quantity | 100 BTC |
| Max Position Size | 1000 BTC |
| Max Open Orders | 100 |
| Query Orders Limit | 500 |
| Query Open Orders Limit | 1200 |
| Query Trades Limit | 500 |
| JWT Token Validity | 7 days (604800 seconds) |
| WebSocket Ping Interval | 10 seconds |
| WebSocket Timeout | 5 minutes |
