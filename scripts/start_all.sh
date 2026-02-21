#!/usr/bin/env bash
# Start backend, frontend and market-data via docker compose
set -euo pipefail

docker compose up -d --build

echo "Services started. Use 'docker compose ps' to inspect." 
