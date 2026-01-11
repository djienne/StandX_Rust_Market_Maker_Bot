"""
StandX Authentication Module
Handles wallet signature authentication to obtain JWT tokens
"""

import json
import logging
import os
from typing import Optional, Tuple
import httpx
import base58
from nacl.signing import SigningKey
from eth_account import Account
from eth_account.messages import encode_defunct

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StandXAuth:
    """Authentication handler for StandX API."""

    AUTH_BASE_URL = "https://api.standx.com"

    def __init__(self, chain: str = "bsc"):
        """
        Initialize auth handler.

        Args:
            chain: Blockchain to use ('bsc' or 'solana')
        """
        self.chain = chain
        self.ed25519_keypair: Optional[SigningKey] = None
        self.request_id: Optional[str] = None
        self.jwt_token: Optional[str] = None

    def generate_ed25519_keypair(self) -> Tuple[SigningKey, str]:
        """
        Generate ed25519 keypair and request ID.

        Returns:
            Tuple of (SigningKey, request_id)
        """
        # Generate random ed25519 keypair
        self.ed25519_keypair = SigningKey.generate()
        public_key_bytes = self.ed25519_keypair.verify_key.encode()

        # Base58 encode the public key for request ID
        self.request_id = base58.b58encode(public_key_bytes).decode()

        logger.info(f"Generated ed25519 keypair, request_id: {self.request_id[:20]}...")
        return self.ed25519_keypair, self.request_id

    async def prepare_signin(self, wallet_address: str) -> dict:
        """
        Step 1: Request signature data from StandX.

        Args:
            wallet_address: Your wallet address

        Returns:
            Response containing signedData JWT
        """
        if not self.request_id:
            self.generate_ed25519_keypair()

        url = f"{self.AUTH_BASE_URL}/v1/offchain/prepare-signin"
        params = {"chain": self.chain}
        payload = {
            "address": wallet_address,
            "requestId": self.request_id
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                params=params,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            data = response.json()

        logger.info("Received signedData from prepare-signin")
        return data

    def decode_jwt_payload(self, jwt_token: str) -> dict:
        """Decode JWT payload (without verification)."""
        import base64

        parts = jwt_token.split('.')
        if len(parts) != 3:
            raise ValueError("Invalid JWT format")

        # Decode payload (middle part)
        payload_b64 = parts[1]
        # Add padding if needed
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += '=' * padding

        payload_bytes = base64.urlsafe_b64decode(payload_b64)
        return json.loads(payload_bytes)

    def sign_message_eth(self, message: str, private_key: str) -> str:
        """
        Sign a message using Ethereum wallet.

        Args:
            message: Message to sign
            private_key: Wallet private key (with or without 0x prefix)

        Returns:
            Signature string
        """
        if not private_key.startswith('0x'):
            private_key = '0x' + private_key

        account = Account.from_key(private_key)
        message_encoded = encode_defunct(text=message)
        signed = account.sign_message(message_encoded)

        return signed.signature.hex()

    async def login(
        self,
        signature: str,
        signed_data: str,
        expires_seconds: int = 604800
    ) -> dict:
        """
        Step 2: Login with signature to obtain JWT token.

        Args:
            signature: Wallet signature of the message
            signed_data: JWT from prepare_signin
            expires_seconds: Token validity in seconds (default 7 days)

        Returns:
            Response containing JWT token
        """
        url = f"{self.AUTH_BASE_URL}/v1/offchain/login"
        params = {"chain": self.chain}
        payload = {
            "signature": signature,
            "signedData": signed_data,
            "expiresSeconds": expires_seconds
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                params=params,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            data = response.json()

        self.jwt_token = data.get("token")
        logger.info(f"Login successful, token received for {data.get('address')}")
        return data

    async def authenticate(
        self,
        wallet_address: str,
        private_key: str,
        expires_seconds: int = 604800
    ) -> str:
        """
        Complete authentication flow.

        Args:
            wallet_address: Your wallet address
            private_key: Wallet private key
            expires_seconds: Token validity in seconds

        Returns:
            JWT token
        """
        # Step 1: Prepare signin
        prepare_response = await self.prepare_signin(wallet_address)
        signed_data = prepare_response.get("signedData")

        if not signed_data:
            raise ValueError("No signedData in response")

        # Step 2: Decode and get message to sign
        payload = self.decode_jwt_payload(signed_data)
        message = payload.get("message")

        if not message:
            raise ValueError("No message in JWT payload")

        logger.info("Message to sign received")

        # Step 3: Sign the message
        signature = self.sign_message_eth(message, private_key)
        logger.info("Message signed")

        # Step 4: Login
        login_response = await self.login(
            signature=signature,
            signed_data=signed_data,
            expires_seconds=expires_seconds
        )

        return login_response.get("token")

    def create_body_signature(
        self,
        request_id: str,
        timestamp: str,
        payload: str
    ) -> str:
        """
        Create body signature for authenticated requests.

        Args:
            request_id: Unique request ID (UUID)
            timestamp: Timestamp in milliseconds
            payload: JSON string of request body

        Returns:
            Base64 encoded signature
        """
        import base64

        if not self.ed25519_keypair:
            raise ValueError("ed25519 keypair not generated")

        # Build message: v1,{requestId},{timestamp},{payload}
        message = f"v1,{request_id},{timestamp},{payload}"
        message_bytes = message.encode()

        # Sign with ed25519
        signed = self.ed25519_keypair.sign(message_bytes)
        signature = signed.signature

        # Base64 encode
        return base64.b64encode(signature).decode()


async def main():
    """
    Example authentication flow.

    Set these environment variables:
    - STANDX_WALLET_ADDRESS: Your wallet address
    - STANDX_PRIVATE_KEY: Your wallet private key
    """
    wallet_address = os.environ.get("STANDX_WALLET_ADDRESS")
    private_key = os.environ.get("STANDX_PRIVATE_KEY")

    if not wallet_address or not private_key:
        print("=" * 60)
        print("STANDX AUTHENTICATION")
        print("=" * 60)
        print("\nTo authenticate, set these environment variables:")
        print("  export STANDX_WALLET_ADDRESS='0xYourWalletAddress'")
        print("  export STANDX_PRIVATE_KEY='your_private_key'")
        print("\nThen run this script again.")
        print("\nThe JWT token will be printed and can be used with:")
        print("  export STANDX_JWT_TOKEN='<token>'")
        print("=" * 60)
        return

    auth = StandXAuth(chain="bsc")

    try:
        token = await auth.authenticate(
            wallet_address=wallet_address,
            private_key=private_key,
            expires_seconds=604800  # 7 days
        )

        print("\n" + "=" * 60)
        print("AUTHENTICATION SUCCESSFUL")
        print("=" * 60)
        print(f"\nJWT Token:\n{token}")
        print("\nTo use this token, run:")
        print(f"  export STANDX_JWT_TOKEN='{token}'")
        print("=" * 60)

    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        raise


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
