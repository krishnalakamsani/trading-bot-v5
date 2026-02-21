#!/usr/bin/env bash
# Start market-data runner locally (no docker). Useful for debugging.
set -euo pipefail

cd "$(dirname "$0")/.."
python3 market-data-service/mds_service_runner.py
