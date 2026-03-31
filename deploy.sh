#!/bin/bash

# --- Load Configuration from .env ---
if [ -f .env ]; then
    export $(grep -E '^DEPLOY_' .env | xargs)
fi

REMOTE_USER="${DEPLOY_USER:-}"
REMOTE_HOST="${DEPLOY_HOST:-}"
SSH_KEY_PATH="${DEPLOY_SSH_KEY:-}"
REMOTE_DIR="${DEPLOY_DIR:-~/standx-bot}"

# Validate required variables
if [ -z "$REMOTE_USER" ] || [ -z "$REMOTE_HOST" ] || [ -z "$SSH_KEY_PATH" ]; then
    echo "Error: Missing deployment configuration in .env"
    echo "Required variables: DEPLOY_USER, DEPLOY_HOST, DEPLOY_SSH_KEY"
    exit 1
fi

# --- Script ---
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

set -e

echo -e "${GREEN}[1/4] Syncing local code to remote server...${NC}"
rsync -avz -e "ssh -i \"$SSH_KEY_PATH\"" \
    --exclude '.git' \
    --exclude 'target' \
    --exclude '*.log' \
    --exclude '*.csv' \
    ./ "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR"

echo -e "${GREEN}[2/4] Building Docker image on remote server...${NC}"
ssh -i "$SSH_KEY_PATH" "$REMOTE_USER@$REMOTE_HOST" << EOF
    set -e
    cd $REMOTE_DIR
    docker compose build --no-cache
EOF

echo -e "${GREEN}[3/4] Stopping existing container (if running)...${NC}"
ssh -i "$SSH_KEY_PATH" "$REMOTE_USER@$REMOTE_HOST" << EOF
    set -e
    cd $REMOTE_DIR
    docker compose down 2>/dev/null || true
EOF

echo -e "${GREEN}[4/4] Starting container...${NC}"
ssh -i "$SSH_KEY_PATH" "$REMOTE_USER@$REMOTE_HOST" << EOF
    set -e
    cd $REMOTE_DIR
    mkdir -p data
    docker compose up -d
EOF

echo -e "${GREEN}Done! Showing logs (Ctrl+C to exit)...${NC}"
echo ""
ssh -t -i "$SSH_KEY_PATH" "$REMOTE_USER@$REMOTE_HOST" "cd $REMOTE_DIR && docker compose logs -f"
