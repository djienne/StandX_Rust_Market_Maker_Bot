# StandX API Overview

## Introduction

StandX offers **REST and WebSocket APIs** for perpetual futures trading with capabilities for:

- Market data access
- Position management
- Trade execution
- Portfolio monitoring

## Base URLs

| Service | URL |
|---------|-----|
| **Auth API** | `https://api.standx.com` |
| **Perps HTTP API** | `https://perps.standx.com` |
| **WebSocket Market Stream** | `wss://perps.standx.com/ws-stream/v1` |
| **WebSocket Order Stream** | `wss://perps.standx.com/ws-api/v1` |
| **Geo/Region Service** | `https://geo.standx.com` |

## Authentication

- **Method**: JWT tokens obtained through wallet signature authentication
- **Supported Chains**: BSC (Binance Smart Chain), Solana
- **Token Validity**: Up to 7 days (configurable)

## Protocol Support

- **REST (HTTP)**: For trading operations, queries, and account management
- **WebSocket**: For real-time data streams and order execution

## Documentation Sections

1. **[Authentication](./authentication.md)** - JWT token auth using wallet signatures
2. **[HTTP API](./http-api.md)** - REST endpoints for trading, user data, market info
3. **[WebSocket API](./websocket.md)** - Real-time data streams and subscriptions
4. **[API Reference](./reference.md)** - Enums, constants, error codes

## Quick Start

### 1. Authenticate

```bash
# Step 1: Prepare signin
POST https://api.standx.com/v1/offchain/prepare-signin?chain=bsc
Content-Type: application/json

{
  "address": "0xYourWalletAddress",
  "requestId": "base58EncodedEd25519PublicKey"
}

# Step 2: Sign the message and login
POST https://api.standx.com/v1/offchain/login?chain=bsc
Content-Type: application/json

{
  "signature": "0xYourSignature",
  "signedData": "JWTFromStep1"
}
```

### 2. Make API Calls

```bash
# Example: Query balance
GET https://perps.standx.com/api/query_balance
Authorization: Bearer <your_jwt_token>
```

### 3. Connect WebSocket

```javascript
const ws = new WebSocket('wss://perps.standx.com/ws-stream/v1');

ws.onopen = () => {
  ws.send(JSON.stringify({
    subscribe: { channel: "price", symbol: "BTC-USD" }
  }));
};
```

## Request Format Requirements

| Type | Format | Example |
|------|--------|---------|
| **Integer parameters** (timestamps) | JSON integers | `1620000000` |
| **Decimal parameters** (prices, quantities) | JSON strings | `"50000.00"` |
| **Timestamps in queries** | ISO 8601 | `"2025-08-11T03:35:25.559151Z"` |

## Response Format

All API responses follow a standard format:

```json
{
  "code": 0,
  "message": "success",
  "request_id": "xxx-xxx-xxx"
}
```

- `code`: Integer (0 = success)
- `message`: String description
- `request_id`: Unique identifier for tracing
