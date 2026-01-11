# StandX Authentication

## Overview

The StandX Perps API uses a wallet signature-based authentication system to obtain JWT access tokens. Two signature flows are supported:

1. **Standard Authentication** - For obtaining JWT tokens
2. **Body Signature Validation** - For request integrity verification

## Prerequisites

- Valid blockchain wallet (address + private key)
- ed25519 algorithm support
- Development environment with cryptographic libraries

## Supported Chains

| Chain | Value |
|-------|-------|
| Binance Smart Chain | `bsc` |
| Solana | `solana` |

---

## Standard Authentication Flow

### Step 1: Prepare Wallet & Generate Keys

1. Create a temporary ed25519 key pair
2. Generate `requestId` by base58-encoding the public key
3. Obtain wallet address and private key

```javascript
import { Keypair } from '@solana/web3.js';
import bs58 from 'bs58';

// Generate ed25519 keypair
const keypair = Keypair.generate();
const requestId = bs58.encode(keypair.publicKey.toBytes());
```

### Step 2: Request Signature Data

**Endpoint:** `POST https://api.standx.com/v1/offchain/prepare-signin?chain={chain}`

**Headers:**
```
Content-Type: application/json
```

**Request Body:**
```json
{
  "address": "0xYourWalletAddress",
  "requestId": "base58EncodedEd25519PublicKey"
}
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| chain | string | Yes | `bsc` or `solana` (query param) |
| address | string | Yes | Wallet address |
| requestId | string | Yes | Base58-encoded ed25519 public key |

**Success Response:**
```json
{
  "success": true,
  "signedData": "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

### Step 3: Parse & Verify Signature Data

The `signedData` is a JWT containing authentication details. You can verify it using StandX's public certificates.

**Certificate Endpoint:** `GET https://api.standx.com/v1/offchain/certs`

**Decoded JWT Payload Example:**
```json
{
  "domain": "standx.com",
  "uri": "https://standx.com",
  "statement": "Sign in with Ethereum to access more StandX features...",
  "version": "1",
  "chainId": 56,
  "nonce": "74Gd7Plf3a1TMVElc",
  "address": "0x...",
  "requestId": "<requestId>",
  "issuedAt": "2025-10-12T17:46:44.731Z",
  "message": "standx.com wants you to sign in with your Ethereum account:\n...",
  "exp": 1760291384,
  "iat": 1760291204
}
```

### Step 4: Sign the Message

Sign `payload.message` using the wallet's private key to generate a signature.

**Example (ethers.js):**
```javascript
import { Wallet } from 'ethers';

const wallet = new Wallet(privateKey);
const signature = await wallet.signMessage(payload.message);
```

**Example (web3.js for Solana):**
```javascript
import nacl from 'tweetnacl';

const messageBytes = new TextEncoder().encode(payload.message);
const signature = nacl.sign.detached(messageBytes, keypair.secretKey);
```

### Step 5: Obtain Access Token

**Endpoint:** `POST https://api.standx.com/v1/offchain/login?chain={chain}`

**Request Body:**
```json
{
  "signature": "0x...",
  "signedData": "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expiresSeconds": 604800
}
```

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| signature | string | Yes | — | Wallet signature of the message |
| signedData | string | Yes | — | JWT from step 2 |
| expiresSeconds | number | No | 604800 | Token lifetime in seconds (7 days default) |

**Success Response:**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "address": "0x...",
  "alias": "user123",
  "chain": "bsc",
  "perpsAlpha": true
}
```

### Step 6: Use Access Token

Include the token in subsequent API requests:

```
Authorization: Bearer <token>
```

---

## Body Signature Flow

For request body integrity verification on sensitive endpoints (order creation, cancellation, etc.), you must sign the request payload with ed25519.

### Message Format

```
{version},{id},{timestamp},{payload}
```

Example:
```
v1,550e8400-e29b-41d4-a716-446655440000,1620000000000,{"symbol":"BTC-USD","side":"buy"}
```

### Required Headers

```json
{
  "Authorization": "Bearer <token>",
  "x-request-sign-version": "v1",
  "x-request-id": "uuid",
  "x-request-timestamp": "timestamp_in_milliseconds",
  "x-request-signature": "base64_encoded_signature"
}
```

### Signature Generation Process

```javascript
import nacl from 'tweetnacl';
import { v4 as uuidv4 } from 'uuid';

// 1. Prepare components
const version = 'v1';
const requestId = uuidv4();
const timestamp = Date.now().toString();
const payload = JSON.stringify({ symbol: 'BTC-USD', side: 'buy', qty: '0.1' });

// 2. Build message
const message = `${version},${requestId},${timestamp},${payload}`;

// 3. Sign with ed25519 private key
const messageBytes = new TextEncoder().encode(message);
const signature = nacl.sign.detached(messageBytes, ed25519PrivateKey);

// 4. Base64 encode
const signatureBase64 = Buffer.from(signature).toString('base64');

// 5. Attach to headers
const headers = {
  'Authorization': `Bearer ${jwtToken}`,
  'x-request-sign-version': version,
  'x-request-id': requestId,
  'x-request-timestamp': timestamp,
  'x-request-signature': signatureBase64
};
```

---

## Session Management

For order tracking between HTTP and WebSocket, use a consistent session ID:

```
x-session-id: <your_custom_session_id>
```

**Note:** Session ID must align between HTTP requests and WebSocket connections.

---

## Security Recommendations

1. **Store private keys securely** - Use environment variables, never hardcode
2. **Use shorter token expiration** - Reduce compromise risk
3. **Implement token refresh** - For long-running sessions
4. **Validate JWT signatures** - Use StandX's public certificates

---

## Technical Details

| Item | Value |
|------|-------|
| **Key Algorithm** | ed25519 (key generation) |
| **JWT Signing** | ES256 |
| **Key Encoding** | Base58 for public keys |
| **Signature Encoding** | Base64 |
| **JWT Standard** | RFC 7519 |
| **Auth Base URL** | `https://api.standx.com` |
