# StandX HTTP API

## Base Information

| Item | Value |
|------|-------|
| **Base URL** | `https://perps.standx.com` |
| **Authentication** | JWT token in `Authorization: Bearer` header |
| **Token Validity** | 7 days |

---

## Authentication Headers

### Standard Auth
```
Authorization: Bearer <your_jwt_token>
```

### Body Signature (Required for Trading Endpoints)
```
x-request-sign-version: v1
x-request-id: <random_string>
x-request-timestamp: <timestamp_in_milliseconds>
x-request-signature: <your_body_signature>
```

### Session Management
```
x-session-id: <your_custom_session_id>
```

---

## Request Format Requirements

| Type | Format | Example |
|------|--------|---------|
| Integer parameters (timestamps) | JSON integers | `1620000000` |
| Decimal parameters (prices, quantities) | JSON strings | `"50000.00"` |

---

# Trade Endpoints

## Create New Order

**Endpoint:** `POST /api/new_order`

**Auth:** Required + Body Signature

**Session ID:** Recommended for order tracking

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| symbol | string | Trading pair (e.g., `BTC-USD`) |
| side | enum | `buy` or `sell` |
| order_type | enum | `limit` or `market` |
| qty | decimal | Quantity (string format) |
| time_in_force | enum | `gtc`, `ioc`, or `alo` |
| reduce_only | boolean | Position reduction flag |

### Optional Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| price | decimal | Required for limit orders |
| cl_ord_id | string | Client order ID (auto-generated if omitted) |
| margin_mode | enum | Must match position (`cross` or `isolated`) |
| leverage | int | Must match position |

### Request Example

```json
{
  "symbol": "BTC-USD",
  "side": "buy",
  "order_type": "limit",
  "qty": "0.1",
  "price": "50000",
  "time_in_force": "gtc",
  "reduce_only": false
}
```

### Response

```json
{
  "code": 0,
  "message": "success",
  "request_id": "xxx-xxx-xxx"
}
```

> **Note:** Response indicates submission, not execution. Monitor "Order Response Stream" via WebSocket for actual status.

---

## Cancel Order

**Endpoint:** `POST /api/cancel_order`

**Auth:** Required + Body Signature

### Parameters (at least one required)

| Parameter | Type |
|-----------|------|
| order_id | int |
| cl_ord_id | string |

### Request Example

```json
{
  "order_id": 2424844
}
```

### Response

```json
{
  "code": 0,
  "message": "success",
  "request_id": "xxx-xxx-xxx"
}
```

---

## Cancel Multiple Orders

**Endpoint:** `POST /api/cancel_orders`

**Auth:** Required + Body Signature

### Parameters (at least one required)

| Parameter | Type |
|-----------|------|
| order_id_list | int[] |
| cl_ord_id_list | string[] |

### Request Example

```json
{
  "order_id_list": [2424844, 2424845, 2424846]
}
```

### Response

```json
[]
```

---

## Change Leverage

**Endpoint:** `POST /api/change_leverage`

**Auth:** Required + Body Signature

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| symbol | string | Trading pair |
| leverage | int | New leverage value |

### Request Example

```json
{
  "symbol": "BTC-USD",
  "leverage": 10
}
```

### Response

```json
{
  "code": 0,
  "message": "success",
  "request_id": "xxx-xxx-xxx"
}
```

---

## Change Margin Mode

**Endpoint:** `POST /api/change_margin_mode`

**Auth:** Required + Body Signature

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| symbol | string | Trading pair |
| margin_mode | enum | `cross` or `isolated` |

### Request Example

```json
{
  "symbol": "BTC-USD",
  "margin_mode": "cross"
}
```

### Response

```json
{
  "code": 0,
  "message": "success",
  "request_id": "xxx-xxx-xxx"
}
```

---

# User Endpoints

## Transfer Margin

**Endpoint:** `POST /api/transfer_margin`

**Auth:** Required + Body Signature

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| symbol | string | Trading pair |
| amount_in | decimal | Amount to transfer |

### Request Example

```json
{
  "symbol": "BTC-USD",
  "amount_in": "1000.0"
}
```

### Response

```json
{
  "code": 0,
  "message": "success",
  "request_id": "xxx-xxx-xxx"
}
```

---

## Query Order

**Endpoint:** `GET /api/query_order`

**Auth:** Required

### Query Parameters (at least one required)

| Parameter | Type |
|-----------|------|
| order_id | int |
| cl_ord_id | string |

### Response Fields

| Field | Type | Description |
|-------|------|-------------|
| id | int | Order ID |
| cl_ord_id | string | Client order ID |
| symbol | string | Trading pair |
| side | string | `buy` or `sell` |
| order_type | string | `limit` or `market` |
| price | decimal | Order price |
| qty | decimal | Order quantity |
| fill_qty | decimal | Filled quantity |
| fill_avg_price | decimal | Average fill price |
| status | string | Order status |
| leverage | string | Leverage applied |
| time_in_force | string | Time in force |
| reduce_only | boolean | Position reduction flag |
| avail_locked | decimal | Available locked funds |
| created_at | timestamp | Creation time (ISO 8601) |
| updated_at | timestamp | Update time (ISO 8601) |

### Response Example

```json
{
  "avail_locked": "3.071880000",
  "cl_ord_id": "01K2BK4ZKQE0C308SRD39P8N9Z",
  "id": 1820682,
  "leverage": "10",
  "order_type": "limit",
  "price": "121900.00",
  "qty": "0.060",
  "reduce_only": false,
  "side": "sell",
  "status": "open",
  "symbol": "BTC-USD",
  "time_in_force": "gtc",
  "fill_qty": "0",
  "fill_avg_price": "0",
  "created_at": "2025-08-11T03:35:25.559151Z",
  "updated_at": "2025-08-11T03:35:25.559151Z"
}
```

---

## Query User Orders

**Endpoint:** `GET /api/query_orders`

**Auth:** Required

### Query Parameters

| Parameter | Type | Default | Max |
|-----------|------|---------|-----|
| symbol | string | - | - |
| status | enum | - | - |
| order_type | enum | - | - |
| start | string (ISO 8601) | - | - |
| end | string (ISO 8601) | - | - |
| last_id | number | - | - |
| limit | number | 100 | 500 |

### Response Example

```json
{
  "page_size": 1,
  "total": 1,
  "result": [
    {
      "avail_locked": "3.071880000",
      "cl_ord_id": "01K2BK4ZKQE0C308SRD39P8N9Z",
      "id": 1820682,
      "leverage": "10",
      "order_type": "limit",
      "price": "121900.00",
      "qty": "0.060",
      "reduce_only": false,
      "side": "sell",
      "status": "new",
      "symbol": "BTC-USD",
      "time_in_force": "gtc",
      "created_at": "2025-08-11T03:35:25.559151Z",
      "updated_at": "2025-08-11T03:35:25.559151Z"
    }
  ]
}
```

---

## Query All Open Orders

**Endpoint:** `GET /api/query_open_orders`

**Auth:** Required

### Query Parameters

| Parameter | Type | Default | Max |
|-----------|------|---------|-----|
| symbol | string | - | - |
| limit | number | 500 | 1200 |

### Response Format

Same as Query User Orders.

---

## Query User Trades

**Endpoint:** `GET /api/query_trades`

**Auth:** Required

### Query Parameters

| Parameter | Type | Default | Max |
|-----------|------|---------|-----|
| symbol | string | - | - |
| side | string | - | - |
| start | string (ISO 8601) | - | - |
| end | string (ISO 8601) | - | - |
| last_id | number | - | - |
| limit | number | 100 | 500 |

### Response Example

```json
{
  "page_size": 1,
  "total": 1,
  "result": [
    {
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
      "created_at": "2025-08-11T03:36:19.352620Z",
      "updated_at": "2025-08-11T03:36:19.352620Z"
    }
  ]
}
```

---

## Query Position Config

**Endpoint:** `GET /api/query_position_config`

**Auth:** Required

### Required Parameters

| Parameter | Type |
|-----------|------|
| symbol | string |

### Response Example

```json
{
  "symbol": "BTC-USD",
  "leverage": 10,
  "margin_mode": "cross"
}
```

---

## Query User Positions

**Endpoint:** `GET /api/query_positions`

**Auth:** Required

### Query Parameters

| Parameter | Type |
|-----------|------|
| symbol | string |

### Response Example

```json
[
  {
    "id": 15,
    "symbol": "BTC-USD",
    "status": "open",
    "qty": "0.940",
    "entry_price": "121737.96",
    "entry_value": "114433.68240",
    "mark_price": "121715.05",
    "position_value": "114412.14700",
    "leverage": "10",
    "margin_mode": "isolated",
    "margin_asset": "DUSD",
    "holding_margin": "11443.3682400",
    "initial_margin": "11443.36824",
    "maint_margin": "2860.30367500",
    "mmr": "3.993223845366698695025800014",
    "liq_price": "112373.50",
    "bankruptcy_price": "109608.01",
    "realized_pnl": "31.61532",
    "upnl": "-21.53540",
    "created_at": "2025-08-10T09:05:50.265265Z",
    "updated_at": "2025-08-10T09:05:50.265265Z",
    "time": "2025-08-11T03:41:40.922818Z"
  }
]
```

### Position Response Fields

| Field | Type | Description |
|-------|------|-------------|
| id | int | Position ID |
| symbol | string | Trading pair |
| status | string | Position status |
| qty | decimal | Position quantity |
| entry_price | decimal | Average entry price |
| entry_value | decimal | Entry notional value |
| mark_price | decimal | Current mark price |
| position_value | decimal | Current position value |
| leverage | string | Leverage |
| margin_mode | string | `cross` or `isolated` |
| margin_asset | string | Margin token |
| holding_margin | decimal | Margin held |
| initial_margin | decimal | Initial margin |
| maint_margin | decimal | Maintenance margin |
| mmr | decimal | Maintenance margin ratio |
| liq_price | decimal | Liquidation price |
| bankruptcy_price | decimal | Bankruptcy price |
| realized_pnl | decimal | Realized PnL |
| upnl | decimal | Unrealized PnL |

---

## Query User Balances

**Endpoint:** `GET /api/query_balance`

**Auth:** Required

### Response Fields

| Field | Type | Description |
|-------|------|-------------|
| isolated_balance | decimal | Isolated wallet total |
| isolated_upnl | decimal | Isolated unrealized PnL |
| cross_balance | decimal | Cross wallet free balance |
| cross_margin | decimal | Cross margin used |
| cross_upnl | decimal | Cross unrealized PnL |
| locked | decimal | Order lock (margin + fee) |
| cross_available | decimal | `cross_balance - cross_margin - locked + cross_upnl` |
| balance | decimal | Total assets = `cross_balance + isolated_balance` |
| upnl | decimal | Total unrealized PnL |
| equity | decimal | Account equity = `balance + upnl` |
| pnl_freeze | decimal | 24h realized PnL |

### Response Example

```json
{
  "isolated_balance": "11443.3682400",
  "isolated_upnl": "-21.53540",
  "cross_balance": "1088575.259316737",
  "cross_margin": "2860.30367500",
  "cross_upnl": "31.61532",
  "locked": "0.000000000",
  "cross_available": "1085746.571",
  "balance": "1100018.627556737",
  "upnl": "10.07992",
  "equity": "1100028.707476657",
  "pnl_freeze": "31.61532"
}
```

> **Note:** `cross_available` may be negative based on PnL and locks.

---

# Public Endpoints (No Auth Required)

## Query Symbol Info

**Endpoint:** `GET /api/query_symbol_info`

### Required Parameters

| Parameter | Type |
|-----------|------|
| symbol | string |

### Response Example

```json
[
  {
    "symbol": "BTC-USD",
    "base_asset": "BTC",
    "base_decimals": 9,
    "quote_asset": "DUSD",
    "quote_decimals": 9,
    "price_tick_decimals": 2,
    "qty_tick_decimals": 3,
    "min_order_qty": "0.001",
    "max_order_qty": "100",
    "max_position_size": "1000",
    "max_open_orders": "100",
    "max_leverage": "20",
    "def_leverage": "10",
    "maker_fee": "0.0001",
    "taker_fee": "0.0004",
    "depth_ticks": "0.01,0.1,1",
    "price_cap_ratio": "0.3",
    "price_floor_ratio": "0.3",
    "enabled": true,
    "created_at": "2025-07-10T05:15:32.089568Z",
    "updated_at": "2025-07-10T05:15:32.089568Z"
  }
]
```

---

## Query Symbol Market

**Endpoint:** `GET /api/query_symbol_market`

### Required Parameters

| Parameter | Type |
|-----------|------|
| symbol | string |

### Response Example

```json
{
  "symbol": "BTC-USD",
  "base": "BTC",
  "quote": "DUSD",
  "last_price": "121599.94",
  "mid_price": "121599.99",
  "index_price": "121601.158461",
  "mark_price": "121602.43",
  "spread": ["121599.94", "121600.04"],
  "high_price_24h": "122164.08",
  "low_price_24h": "114098.44",
  "volume_24h": "9030.51800000000002509",
  "open_interest": "15.948",
  "funding_rate": "0.00010000",
  "next_funding_time": "2025-08-11T08:00:00Z",
  "time": "2025-08-11T03:44:40.922233Z"
}
```

---

## Query Symbol Price

**Endpoint:** `GET /api/query_symbol_price`

### Required Parameters

| Parameter | Type |
|-----------|------|
| symbol | string |

### Response Example

```json
{
  "symbol": "BTC-USD",
  "base": "BTC",
  "quote": "DUSD",
  "last_price": "121599.94",
  "mid_price": "121599.99",
  "index_price": "121601.158461",
  "mark_price": "121602.43",
  "spread_bid": "121599.94",
  "spread_ask": "121600.04",
  "time": "2025-08-11T03:44:40.922233Z"
}
```

> **Note:** `last_price`, `mid_price`, `spread_ask`, `spread_bid` may be null if no recent trades.

---

## Query Depth Book

**Endpoint:** `GET /api/query_depth_book`

### Required Parameters

| Parameter | Type |
|-----------|------|
| symbol | string |

### Response Example

```json
{
  "symbol": "BTC-USD",
  "asks": [
    ["121895.81", "0.843"],
    ["121896.11", "0.96"]
  ],
  "bids": [
    ["121884.01", "0.001"],
    ["121884.31", "0.001"]
  ]
}
```

---

## Query Recent Trades

**Endpoint:** `GET /api/query_recent_trades`

### Required Parameters

| Parameter | Type |
|-----------|------|
| symbol | string |

### Response Example

```json
[
  {
    "symbol": "BTC-USD",
    "price": "121720.18",
    "qty": "0.01",
    "quote_qty": "1217.2018",
    "is_buyer_taker": true,
    "time": "2025-08-11T03:48:47.086505Z"
  }
]
```

---

## Query Funding Rates

**Endpoint:** `GET /api/query_funding_rates`

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| symbol | string | Trading pair |
| start_time | int | Start time (milliseconds) |
| end_time | int | End time (milliseconds) |

### Response Example

```json
[
  {
    "id": 1,
    "symbol": "BTC-USD",
    "funding_rate": "0.0001",
    "index_price": "121601.158461",
    "mark_price": "121602.43",
    "premium": "0.0001",
    "time": "2025-08-11T03:48:47.086505Z",
    "created_at": "2025-08-11T03:48:47.086505Z",
    "updated_at": "2025-08-11T03:48:47.086505Z"
  }
]
```

---

# Kline Endpoints

## Get Server Time

**Endpoint:** `GET /api/kline/time`

### Response

```
1620000000
```

Unix timestamp integer.

---

## Get Kline History

**Endpoint:** `GET /api/kline/history`

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| symbol | string | Trading pair |
| from | u64 | Start time (seconds) |
| to | u64 | End time (seconds) |
| resolution | enum | Kline interval |

### Optional Parameters

| Parameter | Type |
|-----------|------|
| countBack | u64 |

### Resolution Values

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

### Response Example

```json
{
  "s": "ok",
  "t": [1754897028, 1754897031],
  "c": [121897.95, 121903.04],
  "o": [121896.02, 121898.05],
  "h": [121897.95, 121903.15],
  "l": [121895.92, 121898.05],
  "v": [0.09, 10.542]
}
```

### Response Fields

| Field | Description |
|-------|-------------|
| s | Status |
| t | Timestamps array |
| c | Close prices array |
| o | Open prices array |
| h | High prices array |
| l | Low prices array |
| v | Volumes array |

---

# Health & Utility Endpoints

## Health Check

**Endpoint:** `GET /api/health`

### Response

```
OK
```

Plain text.

---

## Region and Server Time

**Endpoint:** `GET https://geo.standx.com/v1/region`

### Response Example

```json
{
  "systemTime": 1761970177865,
  "region": "jp"
}
```
