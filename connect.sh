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
    echo "Error: Missing connection configuration in .env"
    echo "Required variables: DEPLOY_USER, DEPLOY_HOST, DEPLOY_SSH_KEY"
    exit 1
fi

# Subcommands:
#   ./connect.sh           open interactive shell in $REMOTE_DIR
#   ./connect.sh logs      follow bot logs (Ctrl+C to exit)
#   ./connect.sh status    one-shot container + stats snapshot
#   ./connect.sh <cmd ...> run a remote command in $REMOTE_DIR and exit

case "${1:-shell}" in
    shell)
        exec ssh -t -i "$SSH_KEY_PATH" "$REMOTE_USER@$REMOTE_HOST" \
            "cd $REMOTE_DIR && exec \$SHELL -l"
        ;;
    logs)
        exec ssh -t -i "$SSH_KEY_PATH" "$REMOTE_USER@$REMOTE_HOST" \
            "cd $REMOTE_DIR && docker compose logs -f --tail=100"
        ;;
    status)
        exec ssh -i "$SSH_KEY_PATH" "$REMOTE_USER@$REMOTE_HOST" \
            "cd $REMOTE_DIR && docker compose ps && echo && docker stats --no-stream standx-market-maker"
        ;;
    *)
        exec ssh -t -i "$SSH_KEY_PATH" "$REMOTE_USER@$REMOTE_HOST" \
            "cd $REMOTE_DIR && $*"
        ;;
esac
