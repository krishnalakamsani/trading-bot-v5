import asyncio
import logging
from datetime import datetime, timezone

from config import bot_state, config
from database import save_tick_data
from utils import is_market_open

logger = logging.getLogger(__name__)


class MarketDataService:
    """Continuously fetches LTP data and persists ticks.

    Runs independently of the trading decision loop so that:
    - index/option quotes keep updating even if strategy logic pauses/errors
    - raw data can be stored to DB continuously
    """

    def __init__(self, dhan_api):
        self.dhan = dhan_api
        self.running = False
        self.task = None
        self._last_persist_ts = None
        self._error_backoff_seconds = 0

    async def start(self):
        if self.running:
            return
        self.running = True
        bot_state["market_data_service_active"] = True
        self.task = asyncio.create_task(self._loop())
        logger.info("[MKT] MarketDataService started")

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
            self.task = None
        bot_state["market_data_service_active"] = False
        logger.info("[MKT] MarketDataService stopped")

    def _should_persist_now(self) -> bool:
        # Persist at most once per second by default.
        interval = float(config.get("tick_persist_interval_seconds", 1.0) or 1.0)
        interval = max(0.25, min(10.0, interval))
        now = datetime.now(timezone.utc)
        if self._last_persist_ts is None:
            self._last_persist_ts = now
            return True
        if (now - self._last_persist_ts).total_seconds() >= interval:
            self._last_persist_ts = now
            return True
        return False

    async def _loop(self):
        poll = float(config.get("market_data_poll_seconds", 1.0) or 1.0)
        poll = max(0.25, min(5.0, poll))

        while self.running:
            try:
                index_name = config.get("selected_index", "NIFTY")

                # Option security id only when there is a live position
                option_security_id = None
                pos = bot_state.get("current_position")
                if pos:
                    sec = str(pos.get("security_id") or "")
                    if sec and not sec.startswith("SIM_"):
                        try:
                            option_security_id = int(sec)
                        except Exception:
                            option_security_id = None

                if self.dhan:
                    if option_security_id:
                        index_ltp, option_ltp = await asyncio.to_thread(
                            self.dhan.get_index_and_option_ltp, index_name, option_security_id
                        )
                        if index_ltp and index_ltp > 0:
                            try:
                                from bot_service import get_trading_bot
                                bot = get_trading_bot()
                                if bot and getattr(bot, '_set_index_ltp', None):
                                    bot._set_index_ltp(index_ltp)
                                else:
                                    bot_state["index_ltp"] = float(index_ltp)
                            except Exception:
                                bot_state["index_ltp"] = float(index_ltp)
                        # Broadcast tick to websocket clients (single source of truth)
                        try:
                            from server import manager
                            payload = {"type": "tick", "data": {"index_ltp": float(index_ltp), "source": "market_data_service"}}
                            try:
                                asyncio.create_task(manager.broadcast(payload))
                            except Exception:
                                # fallback to sync
                                await manager.broadcast(payload)
                        except Exception:
                            logger.debug("[MKT] Failed to broadcast tick to websocket clients")
                        if option_ltp and option_ltp > 0:
                            option_ltp = round(float(option_ltp) / 0.05) * 0.05
                            bot_state["current_option_ltp"] = round(option_ltp, 2)

                        if config.get("store_tick_data", True) and self._should_persist_now():
                            asyncio.create_task(
                                save_tick_data(
                                    index_name=index_name,
                                    index_ltp=float(index_ltp or 0.0),
                                    option_security_id=str(option_security_id),
                                    option_ltp=float(option_ltp or 0.0),
                                )
                            )
                        # Also broadcast option LTP updates to websocket clients
                        try:
                            if option_ltp and option_ltp > 0:
                                from server import manager
                                payload = {"type": "option_tick", "data": {"option_ltp": float(option_ltp), "security_id": str(option_security_id), "source": "market_data_service"}}
                                try:
                                    asyncio.create_task(manager.broadcast(payload))
                                except Exception:
                                    await manager.broadcast(payload)
                        except Exception:
                            logger.debug("[MKT] Failed to broadcast option tick")
                    else:
                        index_ltp = await asyncio.to_thread(self.dhan.get_index_ltp, index_name)
                        if index_ltp and index_ltp > 0:
                            try:
                                from bot_service import get_trading_bot
                                bot = get_trading_bot()
                                if bot and getattr(bot, '_set_index_ltp', None):
                                    bot._set_index_ltp(index_ltp)
                                else:
                                    bot_state["index_ltp"] = float(index_ltp)
                            except Exception:
                                bot_state["index_ltp"] = float(index_ltp)
                        # Broadcast tick to websocket clients (single source of truth)
                        try:
                            from server import manager
                            payload = {"type": "tick", "data": {"index_ltp": float(index_ltp), "source": "market_data_service"}}
                            try:
                                asyncio.create_task(manager.broadcast(payload))
                            except Exception:
                                await manager.broadcast(payload)
                        except Exception:
                            logger.debug("[MKT] Failed to broadcast tick to websocket clients")

                        if config.get("store_tick_data", True) and self._should_persist_now():
                            asyncio.create_task(
                                save_tick_data(
                                    index_name=index_name,
                                    index_ltp=float(index_ltp or 0.0),
                                )
                            )

                # Reset backoff on success
                self._error_backoff_seconds = 0

                # Optional: if you only want ticks during market hours, flip this on.
                if config.get("pause_market_data_when_closed", False) and not is_market_open():
                    sleep_for = 5
                else:
                    sleep_for = poll

                # If we're in backoff due to earlier errors, use that (capped)
                if self._error_backoff_seconds and self._error_backoff_seconds > sleep_for:
                    sleep_for = self._error_backoff_seconds

                await asyncio.sleep(sleep_for)

            except asyncio.CancelledError:
                break
            except Exception as e:
                # Exponential backoff to handle transient API errors / rate-limits
                try:
                    if not self._error_backoff_seconds:
                        self._error_backoff_seconds = max(1, int(poll))
                    else:
                        self._error_backoff_seconds = min(60, int(self._error_backoff_seconds * 2))
                except Exception:
                    self._error_backoff_seconds = min(60, (self._error_backoff_seconds or 1) * 2)

                logger.error(f"[MKT] MarketDataService error: {e} - backing off for {self._error_backoff_seconds}s", exc_info=True)
                await asyncio.sleep(self._error_backoff_seconds)
