#!/usr/bin/env bash
# Stop/remove compose stack
set -euo pipefail

docker compose down

echo "Compose stack stopped."