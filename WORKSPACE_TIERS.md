3-Tier Workspace Layout

This repository is organized to follow a 3-tier architecture: frontend, backend (API & bot), and market-data ingestion.

Current mapping (directories):

- `frontend/`  — React UI that connects to backend via REST and WebSocket.
- `backend/`   — FastAPI server, trading bot logic, API, websocket handler.
- `market-data-service/mds_service_runner.py` — standalone runner to run Market Data Service independently.

Recommended services (Docker Compose):

- `frontend` service (existing)
- `backend` service (existing)
- `market-data` service (runs `python3 market-data-service/mds_service_runner.py`)

How to run (docker-compose):


```bash
# start all services (build if needed)
docker compose up -d --build

# tail backend logs
docker compose logs -f backend

# tail market-data logs
docker compose logs -f market-data
```


How to run market-data runner directly (no Docker):

```bash
python3 market-data-service/mds_service_runner.py
```

Notes:
- The `MarketDataService` was extracted to run as a standalone process so it can be scaled, rate-limited and monitored separately from the API server.
- WebSocket clients now receive the same tick data the backend uses (single source of truth).
- If you want a filesystem reorganization (moving `backend/` into `services/backend/`, etc.), I can do that next — it is invasive and will require updating paths in `docker-compose.yml` and potentially import paths.
