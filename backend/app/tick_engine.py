"""TickEngine — polls MDS for latest candle closes, builds OHLC, broadcasts over WebSocket.

Responsibilities (this file only):
  - Poll MDS REST /v1/latest-close every ~1 s
  - Detect new candle closes (timestamp change)
  - Broadcast {"type": "tick", "data": {index, ltp, ts}}   — every poll
  - Broadcast {"type": "candle", "data": {ohlc...}}        — on each new close
  - Expose last_closed_candle + candle_event for trading_bot.run_loop()

What this does NOT do:
  - Talk to Dhan directly (that's MDS's job for index data)
  - Store anything to DB
  - Run any trading logic
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class OHLC:
    open: float
    high: float
    low: float
    close: float
    ts: float           # unix epoch of the candle open (from MDS)


class TickEngine:
    """Polls MDS for new candle closes; notifies the trading loop via candle_event."""

    def __init__(self) -> None:
        self.last_closed_candle: Optional[OHLC] = None
        self.last_tick_ltp: float = 0.0
        # trading loop waits on this; TickEngine sets-then-clears it on each new candle
        self.candle_event: asyncio.Event = asyncio.Event()

        self._task: Optional[asyncio.Task] = None
        self._last_candle_ts: Optional[str] = None   # MDS timestamp string, change = new candle

    # ── lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run(), name="tick_engine")
        logger.info("[TICK] TickEngine started")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[TICK] TickEngine stopped")

    # ── main loop ────────────────────────────────────────────────────────────

    async def _run(self) -> None:
        from config import config, bot_state

        while True:
            try:
                index_name = config.get("selected_index", "NIFTY")
                interval   = int(config.get("candle_interval", 5) or 5)
                base_url   = str(config.get("mds_base_url", "") or "").strip()
                poll_s     = float(config.get("mds_poll_seconds", 1.0) or 1.0)

                if not base_url:
                    await asyncio.sleep(2)
                    continue

                candle = await self._fetch_latest(base_url, index_name, interval, poll_s)

                if candle:
                    close = candle.get("close") or candle.get("ltp") or 0.0
                    ts_str = str(candle.get("ts") or "")
                    high   = float(candle.get("high") or close)
                    low    = float(candle.get("low")  or close)

                    ltp = float(close)
                    if ltp > 0:
                        bot_state["index_ltp"] = ltp
                        self.last_tick_ltp = ltp
                        await self._broadcast_tick(index_name, ltp)

                    # New candle closed when MDS timestamp advances
                    if ts_str and ts_str != self._last_candle_ts and ltp > 0:
                        self._last_candle_ts = ts_str
                        ohlc = OHLC(
                            open  = float(candle.get("open") or ltp),
                            high  = float(high),
                            low   = float(low),
                            close = ltp,
                            ts    = float(candle.get("ts_epoch") or time.time()),
                        )
                        self.last_closed_candle = ohlc
                        await self._broadcast_candle(index_name, interval, ohlc)
                        # Signal trading loop
                        self.candle_event.set()
                        self.candle_event.clear()

                await asyncio.sleep(max(0.5, poll_s))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[TICK] Loop error: {e}")
                await asyncio.sleep(2)

    async def _fetch_latest(self, base_url: str, symbol: str,
                            timeframe_seconds: int, min_poll_s: float) -> Optional[dict]:
        """Call MDS /v1/latest-close and return the candle dict, or None on error."""
        try:
            from mds_client import fetch_latest_candle
            return await fetch_latest_candle(
                base_url=base_url,
                symbol=symbol,
                timeframe_seconds=timeframe_seconds,
            )
        except Exception as e:
            logger.debug(f"[TICK] fetch_latest error: {e}")
            return None

    # ── broadcast helpers ─────────────────────────────────────────────────────

    async def _broadcast_tick(self, index: str, ltp: float) -> None:
        try:
            from server import manager
            asyncio.create_task(manager.broadcast({
                "type": "tick",
                "data": {"index": index, "ltp": ltp, "ts": time.time()},
            }))
        except Exception as e:
            logger.debug(f"[TICK] broadcast_tick error: {e}")

    async def _broadcast_candle(self, index: str, interval: int, c: OHLC) -> None:
        try:
            from server import manager
            asyncio.create_task(manager.broadcast({
                "type": "candle",
                "data": {
                    "index": index, "interval": interval,
                    "open": c.open, "high": c.high, "low": c.low, "close": c.close,
                    "ts": c.ts,
                },
            }))
        except Exception as e:
            logger.debug(f"[TICK] broadcast_candle error: {e}")


# Global singleton — imported by server.py and trading_bot.py
tick_engine = TickEngine()
