#!/bin/bash

# --- Load Configuration from .env ---
if [ -f .env ]; then
    export $(grep -E '^DEPLOY_' .env | xargs)
fi

REMOTE_USER="${DEPLOY_USER:-}"
REMOTE_HOST="${DEPLOY_HOST:-}"
SSH_KEY_PATH="${DEPLOY_SSH_KEY:-}"
REMOTE_DIR="${DEPLOY_DIR:-~/standx-bot}"

if [ -z "$REMOTE_USER" ] || [ -z "$REMOTE_HOST" ] || [ -z "$SSH_KEY_PATH" ]; then
    echo "Error: Missing deployment configuration in .env"
    exit 1
fi

echo "Following logs (Ctrl+C to exit)..."
ssh -t -i "$SSH_KEY_PATH" "$REMOTE_USER@$REMOTE_HOST" "cd $REMOTE_DIR && docker compose logs -f"
