"""FastAPI Server - Thin Controller Layer
Only handles API routes, request validation, and responses.
All business logic is delegated to bot_service and other modules.
"""
from fastapi import FastAPI, APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from datetime import datetime, timezone
from typing import List

# Local imports
from config import ROOT_DIR, bot_state, config
import re

from models import ConfigUpdate, StrategyCreate, StrategyRename, StrategyDuplicate, StrategiesImport
from database import (
    init_db,
    load_config,
    prune_backend_market_data,
    get_trades,
    get_trade_analytics,
    upsert_strategy,
    list_strategies,
    get_strategy,
    delete_strategy,
    rename_strategy,
    duplicate_strategy,
    export_strategies,
    import_strategies,
    mark_strategy_applied,
)
import bot_service

# Configure logging: daily rotating file, secrets masked
# The SecretMaskingFilter redacts Dhan API tokens if they ever appear in a log line.
class _SecretMaskingFilter(logging.Filter):
    """Redact known secrets from log messages before they hit any handler."""
    _MASK = "***REDACTED***"
    _SECRET_KEYS = ("dhan_access_token", "access_token", "dhan_client_id", "ws_auth_token")

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            from config import config as _cfg
            secrets = {
                str(v)
                for k, v in _cfg.items()
                if k in self._SECRET_KEYS and v and len(str(v)) > 4
            }
            msg = record.getMessage()
            for secret in secrets:
                if secret in msg:
                    record.msg = record.msg.replace(secret, self._MASK) if isinstance(record.msg, str) else record.msg
                    record.args = None
        except Exception:
            pass
        return True


_mask_filter = _SecretMaskingFilter()

file_handler = TimedRotatingFileHandler(
    filename=str(ROOT_DIR / 'logs' / 'bot.log'),
    when='midnight',
    interval=1,
    backupCount=0,
    encoding='utf-8',
)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
file_handler.addFilter(_mask_filter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
console_handler.addFilter(_mask_filter)
logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
logger = logging.getLogger(__name__)

# Reduce noisy per-request logs from http clients (used for MDS polling).
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Ensure directories exist
(ROOT_DIR / 'logs').mkdir(exist_ok=True)
(ROOT_DIR / 'data').mkdir(exist_ok=True)


# WebSocket Connection Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        try:
            client = getattr(websocket, 'client', None)
            logger.info(f"[WS] Client connected: {client} | Total={len(self.active_connections)}")
        except Exception:
            logger.info(f"[WS] Client connected | Total={len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            try:
                client = getattr(websocket, 'client', None)
                logger.info(f"[WS] Client disconnected: {client} | Total={len(self.active_connections)}")
            except Exception:
                logger.info(f"[WS] Client disconnected | Total={len(self.active_connections)}")

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return

        try:
            logger.debug(f"[WS] Broadcasting message type={message.get('type')} to {len(self.active_connections)} clients")
        except Exception:
            logger.debug("[WS] Broadcasting message to clients")

        # Send in a bounded way; drop broken/slow sockets to avoid log spam.
        stale: List[WebSocket] = []
        for connection in list(self.active_connections):
            client = getattr(connection, 'client', None)
            try:
                await asyncio.wait_for(connection.send_json(message), timeout=5)
            except asyncio.TimeoutError as te:
                stale.append(connection)
                try:
                    logger.warning(f"[WS] Broadcast timeout for client={client}; message_type={message.get('type')} size={len(str(message))} chars; dropping client: {te}")
                except Exception:
                    logger.warning(f"[WS] Broadcast timeout; dropping client: {client}")
            except Exception as e:
                stale.append(connection)
                # Log full exception with traceback to diagnose underlying cause (network/proxy/closed socket)
                try:
                    logger.exception(f"[WS] Broadcast failed for client={client}; message_type={message.get('type')} size={len(str(message))} chars; dropping client")
                except Exception:
                    logger.exception(f"[WS] Broadcast failed; dropping client")

        for ws in stale:
            self.disconnect(ws)

        logger.debug("[WS] Broadcast complete")


manager = ConnectionManager()


# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await load_config()
    logger.info(f"[STARTUP] DB + config loaded. Index={config.get('selected_index', 'NIFTY')}")

    # Prune stale tick/candle rows from backend SQLite (trades DB only, not MDS)
    if bool(config.get('prune_db_on_startup', True)):
        try:
            result = await prune_backend_market_data()
            logger.info(f"[STARTUP] DB prune: {result}")
        except Exception as e:
            logger.warning(f"[STARTUP] DB prune skipped: {e}")

    # Start TickEngine — polls MDS every ~1s, builds candles, broadcasts tick+candle
    # over WebSocket. Trading loop waits on candle_event from this engine.
    try:
        from tick_engine import tick_engine
        await tick_engine.start()
        logger.info("[STARTUP] TickEngine started")
    except Exception as e:
        logger.warning(f"[STARTUP] TickEngine not started: {e}")

    # Optional: auto-start the trading bot on server boot
    if bool(config.get('auto_start_bot', False)):
        try:
            import bot_service
            asyncio.create_task(bot_service.start_bot())
            logger.info("[STARTUP] Auto-start requested for trading bot")
        except Exception as e:
            logger.exception(f"[STARTUP] Failed to auto-start bot: {e}")

    try:
        yield
    finally:
        try:
            from tick_engine import tick_engine
            await tick_engine.stop()
        except Exception:
            pass
        logger.info("[SHUTDOWN] Server shut down")


app = FastAPI(lifespan=lifespan)
api_router = APIRouter(prefix="/api")


def _filter_strategy_config(candidate: dict) -> dict:
    """Allow only known config keys; never persist/apply credentials."""
    if not isinstance(candidate, dict):
        return {}

    allowed_keys = set(config.keys())
    disallowed = {"dhan_access_token", "dhan_client_id"}
    allowed_keys -= disallowed

    filtered = {}
    for k, v in candidate.items():
        if k in allowed_keys:
            filtered[k] = v
    return filtered


def _validate_strategy_name(name: str) -> str:
    name = str(name or "").strip()
    if not name:
        raise ValueError("Strategy name is required")
    if len(name) > 60:
        raise ValueError("Strategy name too long (max 60 chars)")
    # Allow letters, numbers, spaces, and a few safe separators
    if not re.match(r"^[A-Za-z0-9 _\-\.\+\(\)\[\]]+$", name):
        raise ValueError("Strategy name contains unsupported characters")
    return name


def _validate_strategy_config(cfg: dict) -> None:
    if not isinstance(cfg, dict):
        raise ValueError("Strategy config must be an object")

    st_period = int(cfg.get("supertrend_period", 7) or 7)
    if st_period < 1 or st_period > 200:
        raise ValueError("supertrend_period out of range")

    st_mult = float(cfg.get("supertrend_multiplier", 4) or 4)
    if st_mult <= 0 or st_mult > 50:
        raise ValueError("supertrend_multiplier out of range")

    macd_fast = int(cfg.get("macd_fast", 12) or 12)
    macd_slow = int(cfg.get("macd_slow", 26) or 26)
    macd_sig = int(cfg.get("macd_signal", 9) or 9)
    if macd_fast < 1 or macd_slow < 1 or macd_sig < 1:
        raise ValueError("MACD periods must be >= 1")
    if macd_fast >= macd_slow:
        raise ValueError("macd_fast must be less than macd_slow")

    ind = str(cfg.get("indicator_type", "score_mds") or "").lower()
    if ind not in ("score_mds",):
        raise ValueError("indicator_type must be 'score_mds'")

    # ADX validation (used by supertrend_adx)
    if "adx_period" in cfg and cfg["adx_period"] is not None:
        v = int(cfg["adx_period"])
        if v < 1 or v > 200:
            raise ValueError("adx_period out of range")

    if "adx_threshold" in cfg and cfg["adx_threshold"] is not None:
        v = float(cfg["adx_threshold"])
        if v < 0 or v > 100:
            raise ValueError("adx_threshold out of range")

    for key in ("min_trade_gap", "min_hold_seconds", "min_order_cooldown_seconds"):
        if key in cfg and cfg[key] is not None:
            v = int(cfg[key])
            if v < 0 or v > 3600:
                raise ValueError(f"{key} out of range")

    if "htf_filter_timeframe" in cfg and cfg["htf_filter_timeframe"] is not None:
        v = int(cfg["htf_filter_timeframe"])
        # Current backend implementation constrains this to 60s.
        if v != 60:
            raise ValueError("htf_filter_timeframe currently supports only 60 seconds")


# ==================== API Routes ====================

@api_router.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Trading Bot API", "status": "running"}


@api_router.get("/status")
async def get_status():
    """Get bot status"""
    return bot_service.get_bot_status()


@api_router.get("/market/nifty")
async def get_market_data():
    """Get market data (index LTP, SuperTrend)"""
    return bot_service.get_market_data()


@api_router.get("/position")
async def get_position():
    """Get current position"""
    return bot_service.get_position()


@api_router.get("/trades")
async def get_trades_list(limit: int = Query(default=None, le=10000)):
    """Get trade history. Pass limit=None to get all trades"""
    return await get_trades(limit)


@api_router.get("/analytics")
async def get_analytics():
    """Get comprehensive trade analytics and statistics"""
    return await get_trade_analytics()


@api_router.get("/summary")
async def get_summary():
    """Get daily summary"""
    return bot_service.get_daily_summary()


@api_router.get("/logs")
async def get_logs(level: str = Query(default="all"), limit: int = Query(default=100, le=500)):
    """Get bot logs"""
    logs = []
    log_file = ROOT_DIR / 'logs' / 'bot.log'
    
    if log_file.exists():
        with open(log_file, 'r') as f:
            lines = f.readlines()[-limit:]
            for line in lines:
                try:
                    parts = line.strip().split(' - ')
                    if len(parts) >= 4:
                        timestamp = parts[0]
                        log_level = parts[2]
                        message = ' - '.join(parts[3:])
                        
                        if level == "all" or level.upper() == log_level:
                            logs.append({
                                "timestamp": timestamp,
                                "level": log_level,
                                "message": message
                            })
                except Exception:
                    pass
    
    return logs


@api_router.get("/config")
async def get_config():
    """Get current configuration"""
    return bot_service.get_config()


@api_router.post("/debug/ws_test")
async def debug_ws_test():
    """Trigger a test broadcast to all connected WebSocket clients."""
    try:
        from tick_engine import tick_engine
        payload = {
            "type": "debug_test",
            "message": "test broadcast",
            "index_ltp": tick_engine.last_tick_ltp,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await manager.broadcast(payload)
        logger.debug("[DEBUG] Triggered test broadcast")
        return JSONResponse({"status": "ok", "sent": payload})
        return JSONResponse({"status": "ok", "sent": payload})

    except Exception as e:
        logger.exception(f"[DEBUG] Failed to send test broadcast: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/debug/bot_internals")
async def debug_bot_internals():
    """Return internal bot state for debugging (read-only)."""
    try:
        bot = bot_service.get_trading_bot()
    except Exception:
        return JSONResponse(status_code=500, content={"error": "Bot instance not available"})

    try:
        data = {
            "entry_price": getattr(bot, 'entry_price', None),
            "current_option_ltp": bot_state.get('current_option_ltp'),
            "trailing_sl": getattr(bot, 'trailing_sl', None),
            "highest_profit": getattr(bot, 'highest_profit', None),
            "current_position": bot_state.get('current_position'),
            "trail_start_profit": config.get('trail_start_profit'),
            "trail_step": config.get('trail_step'),
            "initial_stoploss": config.get('initial_stoploss'),
        }
        return data
    except Exception as e:
        logger.exception(f"[DEBUG] Failed to read bot internals: {e}")
        return JSONResponse(status_code=500, content={"error": "failed to read internals"})


@api_router.post("/debug/force_trailing_check")
async def debug_force_trailing_check():
    """Force a trailing SL check now for the running bot (for debugging)."""
    try:
        bot = bot_service.get_trading_bot()
    except Exception:
        return JSONResponse(status_code=500, content={"error": "Bot instance not available"})

    try:
        if not bot.current_position:
            return JSONResponse(status_code=400, content={"error": "No open position"})
        ltp = bot_state.get('current_option_ltp') or 0.0
        await bot.check_trailing_sl(float(ltp))
        return {"status": "ok", "checked_ltp": ltp, "trailing_sl": getattr(bot, 'trailing_sl', None), "highest_profit": getattr(bot, 'highest_profit', None)}
    except Exception as e:
        logger.exception(f"[DEBUG] Force trailing check failed: {e}")
        return JSONResponse(status_code=500, content={"error": "failed"})


@api_router.get("/indices")
async def get_indices():
    """Get available indices"""
    return bot_service.get_available_indices_list()


@api_router.get("/candles")
async def get_candles(limit: int = Query(default=1000, le=10000), index_name: str = Query(default=None)):
    """Get historical candle data for analysis"""
    from database import get_candle_data
    return await get_candle_data(limit=limit, index_name=index_name)


@api_router.get("/timeframes")
async def get_timeframes():
    """Get available timeframes"""
    return bot_service.get_available_timeframes()


@api_router.post("/config/update")
async def update_config(update: ConfigUpdate):
    """Update configuration"""
    return await bot_service.update_config_values(update.model_dump(exclude_none=True))


@api_router.post("/config/mode")
async def set_mode(mode: str = Query(..., regex="^(paper|live)$")):
    """Set trading mode"""
    result = await bot_service.set_trading_mode(mode)
    if result.get('status') == 'error':
        raise HTTPException(status_code=400, detail=result['message'])
    return result


@api_router.post("/bot/start")
async def start_bot():
    """Start the trading bot"""
    return await bot_service.start_bot()


@api_router.post("/bot/stop")
async def stop_bot():
    """Stop the trading bot"""
    return await bot_service.stop_bot()


@api_router.post("/bot/squareoff")
async def squareoff():
    """Force square off position"""
    return await bot_service.squareoff_position()


# ==================== Strategies ====================

@api_router.get("/strategies")
async def get_strategies():
    """List saved strategies"""
    return await list_strategies()


@api_router.post("/strategies")
async def save_strategy(payload: StrategyCreate):
    """Save a named strategy (config snapshot).

    If payload.config is omitted, uses current backend config snapshot.
    Credentials are never stored.
    """
    name = _validate_strategy_name(payload.name)
    snapshot = payload.config if payload.config is not None else dict(config)
    safe = _filter_strategy_config(snapshot)
    _validate_strategy_config(safe)
    try:
        result = await upsert_strategy(name, safe)
        return {"status": "success", "strategy": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@api_router.delete("/strategies/{strategy_id}")
async def remove_strategy(strategy_id: int):
    ok = await delete_strategy(strategy_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return {"status": "success"}


@api_router.patch("/strategies/{strategy_id}")
async def update_strategy_name(strategy_id: int, payload: StrategyRename):
    try:
        new_name = _validate_strategy_name(payload.name)
        result = await rename_strategy(strategy_id, new_name)
        return {"status": "success", "strategy": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@api_router.post("/strategies/{strategy_id}/duplicate")
async def duplicate_strategy_api(strategy_id: int, payload: StrategyDuplicate):
    try:
        new_name = _validate_strategy_name(payload.name)
        result = await duplicate_strategy(strategy_id, new_name)
        return {"status": "success", "strategy": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@api_router.get("/strategies/export")
async def export_strategies_api():
    return {"strategies": await export_strategies()}


@api_router.post("/strategies/import")
async def import_strategies_api(payload: StrategiesImport):
    # Filter + validate each item
    cleaned = []
    for item in payload.strategies or []:
        if not isinstance(item, dict):
            continue
        try:
            name = _validate_strategy_name(item.get("name"))
        except Exception:
            continue
        safe = _filter_strategy_config(item.get("config") or {})
        try:
            _validate_strategy_config(safe)
        except Exception:
            continue
        cleaned.append({"name": name, "config": safe})

    result = await import_strategies(cleaned)
    return {"status": "success", **result}


@api_router.post("/strategies/{strategy_id}/apply")
async def apply_strategy(strategy_id: int, start: bool = Query(default=False)):
    """Apply a saved strategy to current config. Optionally start the bot."""
    if bot_state.get("is_running"):
        raise HTTPException(status_code=400, detail="Stop the bot before applying a strategy")
    if bot_state.get("current_position"):
        raise HTTPException(status_code=400, detail="Close position before applying a strategy")

    strategy = await get_strategy(strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")

    safe_updates = _filter_strategy_config(strategy.get("config") or {})
    _validate_strategy_config(safe_updates)
    result = await bot_service.update_config_values(safe_updates)
    if result.get("status") != "success":
        return {"status": "error", "message": "Failed to apply strategy", "result": result}

    await mark_strategy_applied(strategy_id)

    if start:
        start_result = await bot_service.start_bot()
        return {
            "status": "success",
            "message": f"Applied strategy '{strategy.get('name')}' and started bot",
            "strategy": {"id": strategy.get("id"), "name": strategy.get("name")},
            "apply": result,
            "start": start_result,
        }

    return {
        "status": "success",
        "message": f"Applied strategy '{strategy.get('name')}'",
        "strategy": {"id": strategy.get("id"), "name": strategy.get("name")},
        "apply": result,
    }


# ==================== WebSocket ====================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Single WebSocket endpoint.

    Client messages:
      "ping"                                    → "pong"
      {"type": "subscribe", "index": "NIFTY"}  → ack + redirects TickEngine subscription
    
    Server messages:
      {"type": "tick",         "data": {"index", "ltp", "ts"}}
      {"type": "candle",       "data": {"index", "interval", "open", "high", "low", "close", "ts"}}
      {"type": "state_update", "data": {...}}
      {"type": "heartbeat",    "timestamp": "..."}
      {"type": "daily_stop_triggered", "data": {...}}
    """
    # Optional token-based auth
    try:
        token = websocket.query_params.get('token') if getattr(websocket, 'query_params', None) is not None else None
    except Exception:
        token = None

    expected = config.get('ws_auth_token') or ''
    if expected:
        if not token or str(token) != str(expected):
            try:
                await websocket.accept()
                await websocket.send_json({"type": "error", "message": "unauthorized"})
                await websocket.close(code=1008)
            except Exception:
                pass
            logger.warning(f"[WS] Unauthorized connection attempt from {getattr(websocket, 'client', None)}")
            return

    await manager.connect(websocket)
    client = getattr(websocket, 'client', None)
    logger.info(f"[WS] Handler started for {client}")

    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    await websocket.send_text("pong")
                else:
                    try:
                        import json as _json
                        msg = _json.loads(data)
                        if isinstance(msg, dict) and msg.get('type') == 'subscribe':
                            # Client requests subscription to a specific index.
                            # Update the TickEngine — all clients share the same feed.
                            index = str(msg.get('index') or config.get('selected_index', 'NIFTY')).upper()
                            interval = int(msg.get('interval') or config.get('candle_interval', 5) or 5)
                            try:
                                from tick_engine import tick_engine
                                tick_engine.subscribe(index_name=index, candle_interval=interval)
                                logger.info(f"[WS] Client {client} subscribed to {index}/{interval}s")
                            except Exception as e:
                                logger.warning(f"[WS] Subscribe failed: {e}")
                            await websocket.send_json({
                                "type": "ack",
                                "status": "subscribed",
                                "index": index,
                                "interval": interval,
                            })
                        else:
                            logger.debug(f"[WS] Ignoring unsupported message type from {client}")
                    except Exception:
                        logger.debug(f"[WS] Non-JSON message from {client}; ignoring")

            except asyncio.TimeoutError:
                # No message for 30s — send heartbeat to keep connection alive
                try:
                    hb = {"type": "heartbeat", "timestamp": datetime.now(timezone.utc).isoformat()}
                    await websocket.send_json(hb)
                except Exception as e:
                    logger.warning(f"[WS] Heartbeat failed for {client}: {e}")
                    break

    except WebSocketDisconnect:
        logger.info(f"[WS] Client disconnected: {client}")
    except Exception as e:
        logger.exception(f"[WS] Unexpected error for {client}: {e}")
    finally:
        manager.disconnect(websocket)


# Include router and middleware
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
