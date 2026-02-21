"""Trading Bot Engine
Handles all trading logic, signal processing, and order execution.
Uses structured logging with tags for easy troubleshooting.
"""
import asyncio
from datetime import datetime, timezone, timedelta
import logging
import random
import math

from config import bot_state, config, DB_PATH
from indices import get_index_config, round_to_strike
from utils import get_ist_time, is_market_open, can_take_new_trade, should_force_squareoff, format_timeframe
from indicators import SuperTrend, MACD, ADX
from score_engine import ScoreEngine, Candle
from strategies.runner import ScoreMdsRunner
from strategies.runtime import ClosedCandleContext, ScoreMdsRuntime, build_strategy_runtime
from dhan_api import DhanAPI
from database import save_trade, update_trade_exit
from bot_state_machine import state_machine, BotPhase

logger = logging.getLogger(__name__)


class TradingBot:
    """Main trading bot engine"""
    
    def __init__(self):
        self.running = False
        self.task = None
        self.dhan = None
        self.current_position = None
        self.entry_price = 0.0
        self.trailing_sl = None
        self.highest_profit = 0.0
        self.indicator = None  # Will hold selected indicator
        self.htf_indicator = None  # Higher-timeframe SuperTrend (e.g., 1m filter)
        self.macd = None  # LTF MACD for confirmation
        self.adx = None  # ADX strength filter (optional)
        self.score_engine = None  # Multi-timeframe score engine (optional)
        self._st_runner = None
        self._mds_runner = None
        self._strategy_runtime = None
        self.last_exit_candle_time = None
        self.last_trade_time = None  # For min_trade_gap protection
        self.last_signal = None  # For trade_only_on_flip protection
        self._last_entries_paused_log_time = None
        self.entry_time_utc = None  # datetime for min-hold exit protection
        self.last_order_time_utc = None  # datetime for order cooldown (entry/exit pacing)
        self._paper_replay_candles = []
        self._paper_replay_pos = 0
        self._paper_replay_htf_elapsed = 0
        self._last_mds_candle_ts = None
        self._mds_htf_count = 0
        self._mds_htf_high = 0.0
        self._mds_htf_low = float('inf')
        self._mds_htf_close = 0.0
        self._trailing_task: asyncio.Task | None = None
        self._last_index_ltp: float | None = None
        self._index_ltp_streak: int = 0
        self._initialize_indicator()

    def _set_index_ltp(self, value: float) -> float:
        """Set `bot_state['index_ltp']` with simple stall detection logging."""
        try:
            v = float(value) if value is not None else 0.0
        except Exception:
            v = 0.0

        bot_state['index_ltp'] = v

        # Track repeated identical values to detect a stalled feed
        if self._last_index_ltp is None or v != self._last_index_ltp:
            self._last_index_ltp = v
            self._index_ltp_streak = 1
        else:
            self._index_ltp_streak += 1

        if self._index_ltp_streak >= 10:
            logger.warning(f"[MARKET] index_ltp unchanged for {self._index_ltp_streak} updates: {v}")

        return v

    def _prefetch_candles_needed(self) -> int:
        st_period = int(config.get('supertrend_period', 7) or 7)
        macd_slow = int(config.get('macd_slow', 26) or 26)
        macd_signal = int(config.get('macd_signal', 9) or 9)

        # SuperTrend needs at least `period` candles; MACD needs slow EMA + signal EMA.
        base_needed = max(st_period + 1, macd_slow + macd_signal)

        # If HTF filter is enabled (fixed to 60s in current code), seed enough candles
        # so 1m SuperTrend is also ready.
        interval = int(config.get('candle_interval', 5) or 5)
        if bool(config.get('htf_filter_enabled', True)) and interval < 60 and (60 % max(1, interval) == 0):
            multiple_1m = 60 // max(1, interval)
            base_needed = max(base_needed, multiple_1m * (st_period + 1))

        # If score engine is selected, ensure both base TF and next TF are ready.
        if str(config.get('indicator_type', 'supertrend') or 'supertrend').strip().lower() == 'score_mds':
            try:
                base_tf = int(config.get('candle_interval', 5) or 5)
                if base_tf in ScoreEngine._TF_CHAIN:
                    chain = list(ScoreEngine._TF_CHAIN)
                    next_tf = chain[chain.index(base_tf) + 1]
                    multiple = int(next_tf) // max(1, base_tf)
                    next_tf_needed = max(st_period + 1, macd_slow + macd_signal)
                    base_needed = max(base_needed, multiple * next_tf_needed)
            except Exception:
                pass

        # Small safety cushion for flip/slope computations.
        return int(max(50, base_needed + 5))

    async def _seed_indicators_from_mds_history(self) -> None:
        """Seed ScoreEngine with recent MDS candles so indicators are warm on start/restart."""
        if not bool(config.get('prefetch_candles_on_start', True)):
            return

        base_url = str(config.get('mds_base_url', '') or '').strip()
        if not base_url:
            return

        index_name = str(config.get('selected_index', 'NIFTY') or 'NIFTY').strip().upper()
        interval = int(config.get('candle_interval', 5) or 5)
        limit = self._prefetch_candles_needed()

        try:
            from mds_client import fetch_last_candles

            candles = await fetch_last_candles(
                base_url=base_url,
                symbol=index_name,
                timeframe_seconds=interval,
                limit=limit,
            )
        except Exception as e:
            logger.warning(f"[WARMUP] Prefetch failed (MDS): {e}")
            return

        if not candles:
            logger.info("[WARMUP] No candles returned from MDS (skipping seed)")
            return

        # Reset any MDS-derived HTF aggregation state
        self._mds_htf_count = 0
        self._mds_htf_high = 0.0
        self._mds_htf_low = float('inf')
        self._mds_htf_close = 0.0

        multiple_1m = None
        if bool(config.get('htf_filter_enabled', True)) and interval < 60 and (60 % max(1, interval) == 0):
            multiple_1m = 60 // max(1, interval)

        last_indicator_value = None
        last_signal = None
        last_mds = None

        for row in candles:
            if not isinstance(row, dict):
                continue
            try:
                high = float(row.get('high') or 0.0)
                low = float(row.get('low') or float('inf'))
                close = float(row.get('close') or 0.0)
            except Exception:
                continue

            if close <= 0 or high <= 0 or low == float('inf'):
                continue

            last_indicator_value, last_signal = self.indicator.add_candle(high, low, close)
            if self.macd:
                self.macd.add_candle(high, low, close)
            if self.adx:
                adx_val, _adx_sig = self.adx.add_candle(high, low, close)
                if adx_val is not None:
                    bot_state['adx_value'] = float(adx_val)

            if str(config.get('indicator_type') or '').strip().lower() == 'score_mds' and self.score_engine:
                try:
                    last_mds = self.score_engine.on_base_candle(Candle(high=high, low=low, close=close))
                except Exception:
                    last_mds = None

            if multiple_1m:
                self._mds_htf_count += 1
                self._mds_htf_high = max(self._mds_htf_high, high)
                self._mds_htf_low = min(self._mds_htf_low, low)
                self._mds_htf_close = close

                if self._mds_htf_count >= multiple_1m:
                    htf_value, htf_signal = self.htf_indicator.add_candle(
                        self._mds_htf_high, self._mds_htf_low, self._mds_htf_close
                    )
                    if htf_value:
                        bot_state['htf_supertrend_value'] = htf_value if isinstance(htf_value, (int, float)) else str(htf_value)
                        if htf_signal == 'GREEN':
                            bot_state['htf_signal_status'] = 'buy'
                        elif htf_signal == 'RED':
                            bot_state['htf_signal_status'] = 'sell'
                        else:
                            bot_state['htf_signal_status'] = 'waiting'
                        if htf_signal:
                            bot_state['htf_supertrend_signal'] = htf_signal

                    self._mds_htf_count = 0
                    self._mds_htf_high = 0.0
                    self._mds_htf_low = float('inf')
                    self._mds_htf_close = 0.0

        # Publish last computed values to state so UI doesn't show "waiting" on startup.
        if last_indicator_value:
            bot_state['supertrend_value'] = last_indicator_value if isinstance(last_indicator_value, (int, float)) else str(last_indicator_value)
            if last_signal == 'GREEN':
                bot_state['signal_status'] = 'buy'
            elif last_signal == 'RED':
                bot_state['signal_status'] = 'sell'
            else:
                bot_state['signal_status'] = 'waiting'
            if last_signal:
                bot_state['last_supertrend_signal'] = last_signal

        if self.macd and self.macd.last_macd is not None:
            bot_state['macd_value'] = float(self.macd.last_macd)

        if self.adx and getattr(self.adx, 'adx_values', None):
            try:
                bot_state['adx_value'] = float(self.adx.adx_values[-1])
            except Exception:
                pass

        if last_mds is not None:
            bot_state['mds_score'] = float(last_mds.score)
            bot_state['mds_slope'] = float(last_mds.slope)
            bot_state['mds_acceleration'] = float(last_mds.acceleration)
            bot_state['mds_stability'] = float(last_mds.stability)
            bot_state['mds_confidence'] = float(last_mds.confidence)
            bot_state['mds_is_choppy'] = bool(last_mds.is_choppy)
            bot_state['mds_direction'] = str(last_mds.direction)

        # Prevent immediate re-processing of the last candle on first poll.
        try:
            self._last_mds_candle_ts = str((candles[-1] or {}).get('ts') or '') or None
        except Exception:
            self._last_mds_candle_ts = None

        logger.info(f"[WARMUP] Seeded indicators from MDS history | Candles={len(candles)} Interval={interval}s")

    async def _handle_closed_candle(
        self,
        *,
        index_name: str,
        candle_number: int,
        candle_interval: int,
        high: float,
        low: float,
        close: float,
        current_candle_time: datetime,
    ) -> None:
        if not (high > 0 and low < float('inf') and close > 0):
            return

        indicator_value, signal = self.indicator.add_candle(high, low, close)
        macd_value = 0.0
        if self.macd:
            macd_line, _macd_cross = self.macd.add_candle(high, low, close)
            if macd_line is not None:
                macd_value = float(macd_line)

        adx_value = None
        if self.adx:
            try:
                adx_val, _adx_sig = self.adx.add_candle(high, low, close)
                if adx_val is not None:
                    adx_value = float(adx_val)
            except Exception:
                adx_value = None

        mds_snapshot = None
        if config.get('indicator_type') == 'score_mds' and self.score_engine:
            try:
                mds_snapshot = self.score_engine.on_base_candle(Candle(high=float(high), low=float(low), close=float(close)))

                bot_state['mds_score'] = float(mds_snapshot.score)
                bot_state['mds_slope'] = float(mds_snapshot.slope)
                bot_state['mds_acceleration'] = float(mds_snapshot.acceleration)
                bot_state['mds_stability'] = float(mds_snapshot.stability)
                bot_state['mds_confidence'] = float(mds_snapshot.confidence)
                bot_state['mds_is_choppy'] = bool(mds_snapshot.is_choppy)
                bot_state['mds_direction'] = str(mds_snapshot.direction)
            except Exception as e:
                logger.error(f"[MDS] ScoreEngine update failed: {e}", exc_info=True)
                mds_snapshot = None

        # Update state if indicator is ready
        if indicator_value:
            bot_state['supertrend_value'] = indicator_value if isinstance(indicator_value, (int, float)) else str(indicator_value)
        if self.macd and self.macd.last_macd is not None:
            bot_state['macd_value'] = float(self.macd.last_macd)
        else:
            bot_state['macd_value'] = macd_value

        if adx_value is not None:
            bot_state['adx_value'] = float(adx_value)

        # Update signal status (GREEN="buy", RED="sell", None="waiting")
        if signal == "GREEN":
            bot_state['signal_status'] = "buy"
        elif signal == "RED":
            bot_state['signal_status'] = "sell"
        else:
            bot_state['signal_status'] = "waiting"

        # Always log candle close (even while indicators warm up)
        st_txt = f"{indicator_value:.2f}" if isinstance(indicator_value, (int, float)) else "NA"
        macd_txt = f"{macd_value:.4f}" if isinstance(macd_value, (int, float)) else "NA"
        signal_txt = signal if signal else "NONE"
        logger.info(
            f"[CANDLE CLOSE #{candle_number}] {index_name} | "
            f"H={high:.2f} L={low:.2f} C={close:.2f} | "
            f"ST={st_txt} | MACD={macd_txt} | Signal={signal_txt}"
        )

        # Save candle data for analysis (optional; disabled by default to keep DB small)
        if indicator_value and config.get('store_candle_data', False):
            from database import save_candle_data

            await save_candle_data(
                candle_number=candle_number,
                index_name=index_name,
                high=high,
                low=low,
                close=close,
                supertrend_value=indicator_value,
                macd_value=macd_value,
                signal_status=bot_state['signal_status'],
                interval_seconds=int(config.get('candle_interval', candle_interval) or candle_interval),
            )

        # Candle-close trailing SL/target check regardless of signal readiness
        if self.current_position:
            option_ltp = bot_state['current_option_ltp']
            sl_hit = await self.check_trailing_sl_on_close(option_ltp)
            if sl_hit:
                self.last_exit_candle_time = current_candle_time

        runtime = self._get_strategy_runtime()
        await runtime.on_closed_candle(
            self,
            ClosedCandleContext(
                candle_interval_seconds=int(candle_interval or 0),
                current_candle_time=current_candle_time,
                close=float(close),
                signal=str(signal or '') if signal else None,
                mds_snapshot=mds_snapshot,
            ),
        )

    def _can_place_new_entry_order(self) -> bool:
        cooldown = int(config.get('min_order_cooldown_seconds', 0) or 0)
        if cooldown <= 0 or self.last_order_time_utc is None:
            return True
        elapsed = (datetime.now(timezone.utc) - self.last_order_time_utc).total_seconds()
        return elapsed >= cooldown

    def _remaining_entry_cooldown(self) -> float:
        cooldown = int(config.get('min_order_cooldown_seconds', 0) or 0)
        if cooldown <= 0 or self.last_order_time_utc is None:
            return 0.0
        elapsed = (datetime.now(timezone.utc) - self.last_order_time_utc).total_seconds()
        return max(0.0, cooldown - elapsed)

    def _min_hold_active(self) -> bool:
        min_hold = int(config.get('min_hold_seconds', 0) or 0)
        if min_hold <= 0 or self.entry_time_utc is None or not self.current_position:
            return False
        held = (datetime.now(timezone.utc) - self.entry_time_utc).total_seconds()
        return held < min_hold
    
    def initialize_dhan(self):
        """Initialize Dhan API connection"""
        if config['dhan_access_token'] and config['dhan_client_id']:
            try:
                self.dhan = DhanAPI(config['dhan_access_token'], config['dhan_client_id'])
                logger.info("[MARKET] Dhan API initialized")
                return True
            except Exception as e:
                self.dhan = None
                logger.warning(f"[ERROR] Dhan API init failed: {e}")
                return False
        logger.warning("[ERROR] Dhan API credentials not configured")
        return False

    def _paper_should_use_live_option_quotes(self) -> bool:
        if bot_state.get('mode') != 'paper':
            return False
        if not bool(config.get('paper_use_live_option_quotes', True)):
            return False
        if not (config.get('dhan_access_token') and config.get('dhan_client_id')):
            return False
        # Never mix live option quotes with replay/synthetic testing.
        if bool(config.get('paper_replay_enabled', False)):
            return False
        if bool(config.get('bypass_market_hours', False)):
            return False
        return bool(is_market_open())

    async def _paper_upgrade_sim_position_to_live(self) -> bool:
        """Try switching a SIM_* paper position to a real option security_id.

        This is only attempted during market hours (when configured).
        Returns True if upgraded (and current_option_ltp was updated), else False.
        """
        if not self._paper_should_use_live_option_quotes():
            return False
        if not self.current_position:
            return False

        security_id = str(self.current_position.get('security_id') or '')
        if not security_id.startswith('SIM_'):
            return False

        index_name = str(self.current_position.get('index_name') or config.get('selected_index') or 'NIFTY')
        strike = int(self.current_position.get('strike') or 0)
        option_type = str(self.current_position.get('option_type') or '')
        expiry = str(self.current_position.get('expiry') or '')
        if not (index_name and strike and option_type and expiry):
            return False

        if not self.dhan:
            try:
                self.initialize_dhan()
            except Exception:
                pass
        if not self.dhan:
            return False

        try:
            live_security_id = await self.dhan.get_atm_option_security_id(index_name, strike, option_type, expiry)
            if not live_security_id:
                return False

            option_ltp = await self.dhan.get_option_ltp(
                security_id=live_security_id,
                strike=strike,
                option_type=option_type,
                expiry=expiry,
                index_name=index_name,
            )
            if not option_ltp or float(option_ltp) <= 0:
                return False
            option_ltp = round(float(option_ltp) / 0.05) * 0.05
            option_ltp = round(float(option_ltp), 2)

            # Debug: log SIM->LIVE upgrade quote
            logger.debug(f"[PAPER] SIM->LIVE option_ltp fetched: SecID={live_security_id} LTP={option_ltp} Index={index_name}")

            self.current_position['security_id'] = str(live_security_id)
            bot_state['current_position'] = self.current_position
            bot_state['current_option_ltp'] = option_ltp

            logger.info(
                f"[PAPER] SIM->LIVE quotes enabled | {index_name} {option_type} {strike} | SecID: {live_security_id} | LTP: {option_ltp}"
            )
            try:
                await self.check_trailing_sl(bot_state['current_option_ltp'])
            except Exception:
                logger.debug("[SL] check_trailing_sl failed during SIM->LIVE upgrade")
            return True
        except Exception as e:
            logger.debug(f"[PAPER] SIM->LIVE upgrade failed: {e}")
            return False

    async def _init_paper_replay(self) -> None:
        """Load candle data from DB for after-hours paper replay."""
        try:
            index_name = config.get('selected_index', 'NIFTY')
            interval = int(config.get('candle_interval', 5) or 5)
            date_ist = str(config.get('paper_replay_date_ist', '') or '').strip() or None

            provider = str(config.get('market_data_provider', 'dhan') or 'dhan').strip().lower()
            base_url = str(config.get('mds_base_url', '') or '').strip()

            candles = []
            if provider == 'mds' and base_url and date_ist:
                # Consume-only replay: pull historical candles for the selected IST date
                # from market-data-service (TimescaleDB) and replay them locally.
                from mds_client import fetch_candles_for_ist_date

                candles = await fetch_candles_for_ist_date(
                    base_url=base_url,
                    symbol=index_name,
                    timeframe_seconds=interval,
                    date_ist=date_ist,
                    limit=200000,
                )
            else:
                # Legacy fallback: backend SQLite candle_data table (may be empty in consume-only setups)
                from database import get_candle_data_for_replay

                candles = await get_candle_data_for_replay(
                    index_name=index_name,
                    interval_seconds=interval,
                    date_ist=date_ist,
                    limit=20000,
                )

            self._paper_replay_candles = candles or []
            self._paper_replay_pos = 0
            self._paper_replay_htf_elapsed = 0

            src = "MDS" if (provider == 'mds' and base_url and date_ist) else "SQLITE"
            logger.info(f"[REPLAY] Loaded {len(self._paper_replay_candles)} candles | Source={src} | Index={index_name} Interval={interval}s DateIST={date_ist or 'latest'}")
        except Exception as e:
            self._paper_replay_candles = []
            self._paper_replay_pos = 0
            self._paper_replay_htf_elapsed = 0
            logger.error(f"[REPLAY] Failed to load candles: {e}")
    
    def _initialize_indicator(self):
        """Initialize indicators (SuperTrend + optional MACD confirmation)"""
        try:
            self.indicator = SuperTrend(
                period=config['supertrend_period'],
                multiplier=config['supertrend_multiplier']
            )
            # HTF indicator uses same parameters; timeframe aggregation handled in run_loop
            self.htf_indicator = SuperTrend(
                period=config['supertrend_period'],
                multiplier=config['supertrend_multiplier']
            )
            # MACD used for entry confirmation (if enabled)
            self.macd = MACD(
                fast=int(config.get('macd_fast', 12)),
                slow=int(config.get('macd_slow', 26)),
                signal=int(config.get('macd_signal', 9)),
            )

            self.adx = ADX(period=int(config.get('adx_period', 14) or 14))

            self.score_engine = ScoreEngine(
                st_period=int(config.get('supertrend_period', 7)),
                st_multiplier=float(config.get('supertrend_multiplier', 4)),
                macd_fast=int(config.get('macd_fast', 12)),
                macd_slow=int(config.get('macd_slow', 26)),
                macd_signal=int(config.get('macd_signal', 9)),
                base_timeframe_seconds=int(config.get('candle_interval', 5) or 5),
                bonus_macd_triple=float(config.get('mds_bonus_macd_triple', 1.0) or 0.0),
                bonus_macd_momentum=float(config.get('mds_bonus_macd_momentum', 0.5) or 0.0),
                bonus_macd_cross=float(config.get('mds_bonus_macd_cross', 0.5) or 0.0),
            )
            logger.info(f"[SIGNAL] SuperTrend initialized")

            self._initialize_strategy_runners()
        except Exception as e:
            logger.error(f"[ERROR] Failed to initialize indicator: {e}")
            # Fallback to SuperTrend
            self.indicator = SuperTrend(period=7, multiplier=4)
            self.htf_indicator = SuperTrend(period=7, multiplier=4)
            self.macd = MACD(fast=12, slow=26, signal=9)
            self.adx = ADX(period=int(config.get('adx_period', 14) or 14))
            self.score_engine = ScoreEngine(
                st_period=7,
                st_multiplier=4,
                macd_fast=12,
                macd_slow=26,
                macd_signal=9,
                base_timeframe_seconds=int(config.get('candle_interval', 5) or 5),
                bonus_macd_triple=float(config.get('mds_bonus_macd_triple', 1.0) or 0.0),
                bonus_macd_momentum=float(config.get('mds_bonus_macd_momentum', 0.5) or 0.0),
                bonus_macd_cross=float(config.get('mds_bonus_macd_cross', 0.5) or 0.0),
            )
            logger.info(f"[SIGNAL] SuperTrend (fallback) initialized")

            self._initialize_strategy_runners()

    def _initialize_strategy_runners(self) -> None:
        # Only ScoreMdsRunner is supported now.
        self._st_runner = None

        if self._mds_runner is None:
            self._mds_runner = ScoreMdsRunner()
        else:
            self._mds_runner.reset()

        self._strategy_runtime = build_strategy_runtime(config.get('indicator_type'))

    def _get_st_runner(self):
        # SuperTrend runners removed — return None
        return None

    def _get_mds_runner(self) -> ScoreMdsRunner:
        if self._mds_runner is None:
            self._mds_runner = ScoreMdsRunner()
        return self._mds_runner

    def _get_strategy_runtime(self):
        # Only ScoreMdsRuntime supported
        if self._strategy_runtime is None or not isinstance(self._strategy_runtime, ScoreMdsRuntime):
            self._strategy_runtime = build_strategy_runtime(config.get('indicator_type'))
        return self._strategy_runtime
    
    def reset_indicator(self):
        """Reset the selected indicator"""
        if self.indicator:
            self.indicator.reset()
        if self.htf_indicator:
            self.htf_indicator.reset()
        if self.macd:
            self.macd.reset()
        if self.adx:
            self.adx.reset()
        if self.score_engine:
            self.score_engine.reset()
        else:
            # If score_engine wasn't initialized for some reason, try to initialize now.
            try:
                self.score_engine = ScoreEngine(
                    st_period=int(config.get('supertrend_period', 7)),
                    st_multiplier=float(config.get('supertrend_multiplier', 4)),
                    macd_fast=int(config.get('macd_fast', 12)),
                    macd_slow=int(config.get('macd_slow', 26)),
                    macd_signal=int(config.get('macd_signal', 9)),
                    base_timeframe_seconds=int(config.get('candle_interval', 5) or 5),
                    bonus_macd_triple=float(config.get('mds_bonus_macd_triple', 1.0) or 0.0),
                    bonus_macd_momentum=float(config.get('mds_bonus_macd_momentum', 0.5) or 0.0),
                    bonus_macd_cross=float(config.get('mds_bonus_macd_cross', 0.5) or 0.0),
                )
            except Exception:
                self.score_engine = None

        if self._st_runner is not None:
            try:
                self._st_runner.reset()
            except Exception:
                pass
        if self._mds_runner is not None:
            try:
                self._mds_runner.reset()
            except Exception:
                pass
        self._strategy_runtime = build_strategy_runtime(config.get('indicator_type'))
        self._last_mds_candle_ts = None
        self._mds_htf_count = 0
        self._mds_htf_high = 0.0
        self._mds_htf_low = float('inf')
        self._mds_htf_close = 0.0
        logger.info(f"[SIGNAL] Indicator reset: {config.get('indicator_type', 'supertrend')}")

    def _log_st_entry_block(self, *, reason: str, signal: str, flipped: bool) -> None:
        reason = str(reason or '')
        if reason == 'no_flip':
            logger.info(f"[ENTRY] ✗ Skipping - No SuperTrend flip this candle | Signal={signal}")
            logger.info("[ENTRY_DECISION] NO | Reason=no_flip")
            return
        if reason == 'adx_not_ready':
            logger.info("[ENTRY] ✗ Skipping - ADX not ready yet")
            logger.info("[ENTRY_DECISION] NO | Reason=adx_not_ready")
            return
        if reason == 'adx_below_threshold':
            try:
                logger.info(
                    f"[ENTRY] ✗ Skipping - ADX below threshold | ADX={bot_state.get('adx_value', 0.0):.2f} < {float(config.get('adx_threshold', 25.0) or 25.0):.2f}"
                )
            except Exception:
                logger.info("[ENTRY] ✗ Skipping - ADX below threshold")
            logger.info("[ENTRY_DECISION] NO | Reason=adx_below_threshold")
            return
        if reason == 'macd_not_ready':
            logger.info("[ENTRY] ✗ Skipping - MACD not ready yet")
            logger.info("[ENTRY_DECISION] NO | Reason=macd_not_ready")
            return
        if reason == 'macd_not_confirming_buy':
            if self.macd:
                logger.info(
                    f"[ENTRY] ✗ Skipping - MACD not confirming BUY | MACD={self.macd.last_macd:.4f} SIG={self.macd.last_signal_line:.4f}"
                )
            logger.info("[ENTRY_DECISION] NO | Reason=macd_not_confirming_buy")
            return
        if reason == 'macd_not_confirming_sell':
            if self.macd:
                logger.info(
                    f"[ENTRY] ✗ Skipping - MACD not confirming SELL | MACD={self.macd.last_macd:.4f} SIG={self.macd.last_signal_line:.4f}"
                )
            logger.info("[ENTRY_DECISION] NO | Reason=macd_not_confirming_sell")
            return
        if reason == 'htf_not_ready':
            logger.info("[ENTRY] ✗ Skipping - HTF SuperTrend not ready yet (need 1m candles)")
            logger.info("[ENTRY_DECISION] NO | Reason=htf_not_ready")
            return
        if reason == 'htf_mismatch':
            htf_direction = getattr(self.htf_indicator, 'direction', 0) if self.htf_indicator else 0
            htf_side = 'GREEN' if htf_direction == 1 else 'RED'
            logger.info(f"[ENTRY] ✗ Skipping - HTF filter mismatch | LTF={signal}, HTF={htf_side}")
            logger.info("[ENTRY_DECISION] NO | Reason=htf_mismatch")
            return

        logger.info(f"[ENTRY_DECISION] NO | Reason={reason or 'blocked'}")
    
    def is_within_trading_hours(self) -> bool:
        """Check if current time allows new entries
        
        Returns:
            bool: True if within allowed trading hours, False otherwise
        """
        if config.get('bypass_market_hours', False) or (
            bot_state.get('mode') == 'paper' and config.get('paper_replay_enabled', False)
        ):
            return True

        ist = get_ist_time()
        current_time = ist.time()
        
        # Define trading hours
        NO_ENTRY_BEFORE = datetime.strptime("09:25", "%H:%M").time()  # No entry before 9:25 AM
        NO_ENTRY_AFTER = datetime.strptime("15:10", "%H:%M").time()   # No entry after 3:10 PM
        
        # Check if within allowed hours
        if current_time < NO_ENTRY_BEFORE:
            logger.info(f"[HOURS] Entry blocked - market not open yet (Current: {current_time.strftime('%H:%M')}, Opens: 09:25)")
            return False
        
        if current_time > NO_ENTRY_AFTER:
            logger.info(f"[HOURS] Entry blocked - market closing soon (Current: {current_time.strftime('%H:%M')}, Cutoff: 15:10)")
            return False
        
        logger.debug(f"[HOURS] Trading hours OK (Current: {current_time.strftime('%H:%M')})")
        return True
    
    async def start(self):
        """Start the trading bot"""
        if self.running:
            return {"status": "error", "message": "Bot already running"}

        state_machine.start()
        
        # Only require Dhan in live mode. Paper mode can run without broker SDK.
        if bot_state.get('mode') != 'paper':
            if not self.initialize_dhan():
                return {"status": "error", "message": "Dhan API not available (credentials/SDK)"}
        else:
            # In paper mode, optionally initialize Dhan for live quotes if enabled
            # (e.g., when `paper_use_live_option_quotes` is True or provider is 'dhan').
            try:
                if bool(config.get('paper_use_live_option_quotes', True)) or str(config.get('market_data_provider', 'dhan')).strip().lower() == 'dhan':
                    self.initialize_dhan()
            except Exception:
                # Non-fatal: continue with paper flow even if Dhan init fails
                pass

        # Prepare replay candles for after-hours (or bypass-hours) paper simulation
        replay_enabled = bool(bot_state.get('mode') == 'paper' and config.get('paper_replay_enabled', False))
        if replay_enabled:
            await self._init_paper_replay()
            if not self._paper_replay_candles:
                return {"status": "error", "message": "Paper replay enabled but no candles found (MDS/DB)"}
        
        self.running = True
        bot_state['is_running'] = True
        self.reset_indicator()
        self.last_signal = None

        # Live mode: reconcile with broker before starting the loop.
        # Handles crash-while-in-position: rebuilds state so monitoring resumes.
        if bot_state.get('mode') == 'live' and self.dhan:
            try:
                from broker_reconciler import reconcile_with_broker
                reconciled = await reconcile_with_broker(self)
                if reconciled:
                    logger.info("[STARTUP] Open position found — resuming monitoring")
                    state_machine.warmed_up()
                    state_machine.placing_entry()
                    state_machine.entry_confirmed()
                    self.task = asyncio.create_task(self.run_loop())
                    index_name = config['selected_index']
                    interval = format_timeframe(config['candle_interval'])
                    logger.info(f"[BOT] Resumed — Index: {index_name}, Interval: {interval}, Mode: live")
                    return {"status": "success", "message": f"Bot resumed for {index_name} ({interval}) — open position detected"}
            except Exception as e:
                logger.warning(f"[STARTUP] Reconciliation failed (non-fatal): {e}")

        # Seed indicators from MDS history so ScoreEngine is warm on start.
        # Skip for dated replay or synthetic paper testing.
        synthetic_only = (
            bot_state.get('mode') == 'paper'
            and bool(config.get('bypass_market_hours', False))
            and not bool(config.get('paper_replay_enabled', False))
        )
        if (not replay_enabled) and (not synthetic_only):
            await self._seed_indicators_from_mds_history()

        state_machine.warmed_up()
        self.task = asyncio.create_task(self.run_loop())
        
        index_name = config['selected_index']
        interval = format_timeframe(config['candle_interval'])
        indicator_name = config.get('indicator_type', 'supertrend')
        logger.info(f"[BOT] Started - Index: {index_name}, Timeframe: {interval}, Indicator: {indicator_name}, Mode: {bot_state['mode']}")
        
        return {"status": "success", "message": f"Bot started for {index_name} ({interval})"}
    
    async def stop(self):
        """Stop the trading bot"""
        self.running = False
        bot_state['is_running'] = False
        state_machine.stop()
        if self.task:
            self.task.cancel()
        logger.info("[BOT] Stopped")
        return {"status": "success", "message": "Bot stopped"}
    
    async def squareoff(self):
        """Force square off current position"""
        if not self.current_position:
            return {"status": "error", "message": "No open position"}
        
        index_name = config['selected_index']
        qty = int(self.current_position.get('qty') or 0)
        if qty <= 0:
            index_config = get_index_config(index_name)
            qty = config['order_qty'] * index_config['lot_size']
        
        logger.info(f"[ORDER] Force squareoff initiated for {index_name}")
        
        exit_price = bot_state['current_option_ltp']
        pnl = (exit_price - self.entry_price) * qty
        closed = await self.close_position(exit_price, pnl, "Force Square-off")
        if closed:
            suffix = "(Paper)" if bot_state['mode'] == 'paper' else ""
            return {"status": "success", "message": f"Position squared off {suffix}. PnL: {pnl:.2f}"}
        return {"status": "error", "message": "Squareoff order not filled (position still open)"}
    
    async def close_position(self, exit_price: float, pnl: float, reason: str) -> bool:
        """Close current position and save trade"""
        if not self.current_position:
            return False
        
        trade_id = self.current_position.get('trade_id', '')
        index_name = self.current_position.get('index_name', config['selected_index'])
        option_type = self.current_position.get('option_type', '')
        strike = self.current_position.get('strike', 0)
        security_id = self.current_position.get('security_id', '')
        qty = int(self.current_position.get('qty') or 0)
        if qty <= 0:
            index_config = get_index_config(index_name)
            qty = config['order_qty'] * index_config['lot_size']
        
        exit_order_placed = False
        filled_exit_price = exit_price

        if bot_state['mode'] != 'paper' and self.dhan and security_id:
            existing_exit_order_id = self.current_position.get('exit_order_id')

            try:
                if not existing_exit_order_id:
                    logger.info(f"[ORDER] Placing EXIT SELL order | Trade ID: {trade_id} | Security: {security_id} | Qty: {qty}")
                    result = await self.dhan.place_order(security_id, "SELL", qty, index_name=index_name)

                    if result.get('status') == 'success' and result.get('orderId'):
                        existing_exit_order_id = result.get('orderId')
                        self.current_position['exit_order_id'] = existing_exit_order_id
                        bot_state['current_position'] = self.current_position
                        exit_order_placed = True
                        self.last_order_time_utc = datetime.now(timezone.utc)
                        state_machine.placing_exit()
                        logger.info(f"[ORDER] ✓ EXIT order PLACED | OrderID: {existing_exit_order_id} | Security: {security_id} | Qty: {qty}")
                    else:
                        logger.error(f"[ORDER] ✗ EXIT order FAILED | Trade: {trade_id} | Result: {result}")
                        return False

                verify = await self.dhan.verify_order_filled(
                    order_id=str(existing_exit_order_id),
                    security_id=str(security_id),
                    expected_qty=int(qty),
                    timeout_seconds=30
                )

                if not verify.get('filled'):
                    status = verify.get('status')
                    logger.warning(
                        f"[ORDER] ✗ EXIT not filled yet | Trade: {trade_id} | OrderID: {existing_exit_order_id} | Status: {status} | {verify.get('message')}"
                    )
                    if status in {"REJECTED", "CANCELLED", "ERROR"}:
                        self.current_position.pop('exit_order_id', None)
                        bot_state['current_position'] = self.current_position
                        state_machine.exit_failed()
                    return False

                avg_price = float(verify.get('average_price') or 0)
                if avg_price > 0:
                    filled_exit_price = round(avg_price / 0.05) * 0.05
                    filled_exit_price = round(filled_exit_price, 2)
                exit_order_placed = True

            except Exception as e:
                logger.error(f"[ORDER] ✗ Error placing/verifying EXIT order: {e} | Trade: {trade_id}", exc_info=True)
                return False
        elif not security_id:
            logger.warning(f"[WARNING] Cannot send exit order - security_id missing for {index_name} {option_type} | Trade: {trade_id}")
            logger.info(f"[EXIT] ✓ Position closed | {index_name} {option_type} {strike} | Reason: {reason} | PnL: {pnl} | Order Placed: False")
            # Update DB in background - don't wait
            asyncio.create_task(update_trade_exit(
                trade_id=trade_id,
                exit_time=datetime.now(timezone.utc).isoformat(),
                exit_price=exit_price,
                pnl=pnl,
                exit_reason=reason
            ))
            # Track last order timestamp (treated as an exit action even if no broker order)
            self.last_order_time_utc = datetime.now(timezone.utc)
        elif bot_state['mode'] == 'paper':
            logger.info(f"[ORDER] Paper mode - EXIT order not placed to Dhan (simulated) | Trade: {trade_id}")
            logger.info(f"[EXIT] ✓ Position closed | {index_name} {option_type} {strike} | Reason: {reason} | PnL: {pnl} | Order Placed: False")
            # Update DB in background - don't wait
            asyncio.create_task(update_trade_exit(
                trade_id=trade_id,
                exit_time=datetime.now(timezone.utc).isoformat(),
                exit_price=exit_price,
                pnl=pnl,
                exit_reason=reason
            ))
            # Track last order timestamp (paper exit)
            self.last_order_time_utc = datetime.now(timezone.utc)

        # If we reached here in LIVE mode, the exit is filled. Use filled price for P&L if available.
        if bot_state['mode'] != 'paper' and self.dhan and security_id:
            pnl = (filled_exit_price - self.entry_price) * qty

            asyncio.create_task(update_trade_exit(
                trade_id=trade_id,
                exit_time=datetime.now(timezone.utc).isoformat(),
                exit_price=filled_exit_price,
                pnl=pnl,
                exit_reason=reason
            ))
        
        # Update state
        bot_state['daily_pnl'] += pnl
        bot_state['current_position'] = None
        bot_state['trailing_sl'] = None
        bot_state['entry_price'] = 0
        
        if bot_state['daily_pnl'] < -config['daily_max_loss']:
            bot_state['daily_max_loss_triggered'] = True
            logger.warning(f"[EXIT] Daily max loss triggered! PnL: {bot_state['daily_pnl']:.2f}")
            # Notify frontend immediately so the UI can show a clear banner
            try:
                from server import manager
                asyncio.create_task(manager.broadcast({
                    "type": "daily_stop_triggered",
                    "data": {
                        "daily_pnl": bot_state['daily_pnl'],
                        "daily_max_loss": config['daily_max_loss'],
                        "message": f"Daily max loss of ₹{config['daily_max_loss']} reached. Bot will not take new trades today."
                    }
                }))
            except Exception:
                pass
        
        if pnl < 0 and abs(pnl) > bot_state['max_drawdown']:
            bot_state['max_drawdown'] = abs(pnl)
        
        # Track the signal at exit - require signal change before next entry
        # If we exited CE position, last signal was GREEN
        # If we exited PE position, last signal was RED
        if option_type == 'CE':
            self.last_signal = 'GREEN'
        elif option_type == 'PE':
            self.last_signal = 'RED'
        
        self.current_position = None
        self.entry_price = 0
        self.trailing_sl = None
        self.highest_profit = 0
        self.entry_time_utc = None

        state_machine.exit_confirmed()
        # Cooldown phase ends immediately — min_order_cooldown_seconds is enforced
        # by _can_place_new_entry_order(), so we transition to SCANNING right away.
        state_machine.cooldown_done()

        # Cancel trailing monitor task if running
        try:
            if getattr(self, '_trailing_task', None) is not None:
                try:
                    self._trailing_task.cancel()
                except Exception:
                    pass
                self._trailing_task = None
        except Exception:
            pass

        logger.info(f"[EXIT] ✓ Position closed | {index_name} {option_type} {strike} | Reason: {reason} | PnL: {pnl:.2f} | Order Placed: {exit_order_placed}")
        return True
    
    async def run_loop(self):
        """Main trading loop — driven by TickEngine candle events.

        Architecture:
          - TickEngine fetches index LTP from Dhan every second
          - TickEngine builds OHLC candles and fires candle_event on close
          - This loop wakes on each closed candle, runs indicators, manages trades
          - No Dhan polling happens here; all tick data comes from TickEngine
        """
        from tick_engine import tick_engine

        logger.info("[BOT] Trading loop started (TickEngine-driven)")

        candle_number = 0
        htf_high, htf_low, htf_close = 0.0, float('inf'), 0.0
        htf_elapsed_seconds = 0
        htf_candle_number = 0

        # Make sure TickEngine has the Dhan handle for live quotes
        if self.dhan:
            tick_engine.set_dhan(self.dhan)

        # Subscribe to current index/interval
        index_name = config['selected_index']
        candle_interval = int(config.get('candle_interval', 5) or 5)
        tick_engine.subscribe(index_name=index_name, candle_interval=candle_interval)

        while self.running:
            try:
                index_name = config['selected_index']
                candle_interval = int(config.get('candle_interval', 5) or 5)

                # Keep TickEngine subscription in sync with live config changes
                tick_engine.subscribe(index_name=index_name, candle_interval=candle_interval)

                replay_enabled = (
                    bot_state.get('mode') == 'paper' and bool(config.get('paper_replay_enabled', False))
                )

                # --- Daily reset at 9:15 AM IST ---
                ist = get_ist_time()
                if ist.hour == 9 and ist.minute == 15:
                    last_reset = bot_state.get('last_daily_reset_date')
                    today = ist.date().isoformat()
                    if last_reset != today:
                        bot_state['daily_trades'] = 0
                        bot_state['daily_pnl'] = 0.0
                        bot_state['daily_max_loss_triggered'] = False
                        bot_state['max_drawdown'] = 0.0
                        self.last_exit_candle_time = None
                        self.last_trade_time = None
                        self.last_signal = None
                        candle_number = 0
                        htf_candle_number = 0
                        htf_elapsed_seconds = 0
                        self.reset_indicator()
                        bot_state['last_daily_reset_date'] = today
                        logger.info("[BOT] Daily reset at 9:15 AM")

                # --- Force square-off at 3:25 PM ---
                if (not replay_enabled) and should_force_squareoff() and self.current_position:
                    logger.info("[EXIT] Auto squareoff at 3:25 PM")
                    await self.squareoff()

                # --- Market hours gate ---
                if (not replay_enabled) and (not is_market_open()):
                    await asyncio.sleep(5)
                    continue

                # --- Daily loss gate ---
                if bot_state['daily_max_loss_triggered'] and not self.current_position:
                    await asyncio.sleep(5)
                    continue

                # --- Paper replay path (DB candles, no TickEngine) ---
                if replay_enabled:
                    if self._paper_replay_pos >= len(self._paper_replay_candles):
                        logger.info("[REPLAY] Completed candle replay")
                        self.running = False
                        bot_state['is_running'] = False
                        break

                    row = self._paper_replay_candles[self._paper_replay_pos]
                    self._paper_replay_pos += 1

                    try:
                        high = float(row.get('high') or 0.0)
                        low = float(row.get('low') or float('inf'))
                        close = float(row.get('close') or 0.0)
                    except Exception:
                        high, low, close = 0.0, float('inf'), 0.0

                    if close > 0:
                        self._set_index_ltp(close)

                    # HTF aggregation for replay
                    if config.get('htf_filter_enabled', True) and candle_interval < 60 and close > 0:
                        htf_high = max(htf_high, high)
                        htf_low = min(htf_low, low)
                        htf_close = close
                        htf_seconds = int(config.get('htf_filter_timeframe', 60) or 60)
                        htf_elapsed_seconds += candle_interval
                        if htf_elapsed_seconds >= htf_seconds:
                            htf_candle_number += 1
                            if htf_high > 0 and htf_low < float('inf'):
                                htf_value, htf_signal = self.htf_indicator.add_candle(htf_high, htf_low, htf_close)
                                self._update_htf_state(htf_value, htf_signal)
                            htf_high, htf_low, htf_close = 0.0, float('inf'), 0.0
                            htf_elapsed_seconds = 0

                    if high > 0 and low < float('inf'):
                        candle_number += 1
                        await self._handle_closed_candle(
                            index_name=index_name,
                            candle_number=candle_number,
                            candle_interval=candle_interval,
                            high=high,
                            low=low,
                            close=close,
                            current_candle_time=datetime.now(),
                        )

                    # Simulate option LTP for paper replay positions
                    if self.current_position:
                        self._simulate_option_ltp(index_name)

                    await self.broadcast_state()
                    speed = float(config.get('paper_replay_speed', 10.0) or 10.0)
                    speed = max(0.1, min(100.0, speed))
                    await asyncio.sleep(max(0.05, float(candle_interval) / speed))
                    continue

                # --- Live / paper mode: wait for next closed candle from TickEngine ---
                # Use a short-polling wait so we can still broadcast state every second
                # even if a candle hasn't closed yet (keeps UI LTP live)
                try:
                    await asyncio.wait_for(tick_engine.candle_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    # No new candle yet — update option LTP and broadcast, then loop
                    await self._update_option_ltp_if_needed()
                    await self.broadcast_state()
                    continue

                # A new candle just closed
                candle = tick_engine.last_closed_candle
                if candle is None:
                    await self.broadcast_state()
                    continue

                high = candle.high
                low = candle.low
                close = candle.close

                if not (high > 0 and low < float('inf') and close > 0):
                    await self.broadcast_state()
                    continue

                self._set_index_ltp(close)

                # HTF aggregation from base candles
                if config.get('htf_filter_enabled', True) and candle_interval < 60:
                    htf_high = max(htf_high, high)
                    htf_low = min(htf_low, low)
                    htf_close = close
                    htf_seconds = int(config.get('htf_filter_timeframe', 60) or 60)
                    htf_elapsed_seconds += candle_interval
                    if htf_elapsed_seconds >= htf_seconds:
                        htf_candle_number += 1
                        if htf_high > 0 and htf_low < float('inf'):
                            htf_value, htf_signal = self.htf_indicator.add_candle(htf_high, htf_low, htf_close)
                            self._update_htf_state(htf_value, htf_signal)
                        htf_high, htf_low, htf_close = 0.0, float('inf'), 0.0
                        htf_elapsed_seconds = 0

                candle_number += 1
                await self._handle_closed_candle(
                    index_name=index_name,
                    candle_number=candle_number,
                    candle_interval=candle_interval,
                    high=high,
                    low=low,
                    close=close,
                    current_candle_time=datetime.now(),
                )

                # Tick-level SL check after each candle
                if self.current_position and bot_state['current_option_ltp'] > 0:
                    tick_exit = await self.check_tick_sl(bot_state['current_option_ltp'])
                    if tick_exit:
                        await self.broadcast_state()
                        continue

                # Paper mode: simulate option LTP
                if self.current_position and bot_state.get('mode') == 'paper':
                    self._simulate_option_ltp(index_name)

                await self.broadcast_state()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[ERROR] Trading loop exception: {e}")
                await asyncio.sleep(5)

    def _update_htf_state(self, htf_value, htf_signal) -> None:
        """Write HTF SuperTrend values into bot_state."""
        if htf_value:
            bot_state['htf_supertrend_value'] = htf_value if isinstance(htf_value, (int, float)) else str(htf_value)
        if htf_signal == 'GREEN':
            bot_state['htf_signal_status'] = 'buy'
        elif htf_signal == 'RED':
            bot_state['htf_signal_status'] = 'sell'
        else:
            bot_state['htf_signal_status'] = 'waiting'
        if htf_signal:
            bot_state['htf_supertrend_signal'] = htf_signal

    async def _update_option_ltp_if_needed(self) -> None:
        """Fetch live option LTP from Dhan when a real position is open."""
        if not self.current_position or not self.dhan:
            return
        security_id = self.current_position.get('security_id', '')
        if not security_id or security_id.startswith('SIM_'):
            return
        try:
            index_name = config['selected_index']
            opt_id = int(security_id)
            _idx_ltp, option_ltp = await asyncio.to_thread(
                self.dhan.get_index_and_option_ltp, index_name, opt_id
            )
            if option_ltp and option_ltp > 0:
                option_ltp = round(round(option_ltp / 0.05) * 0.05, 2)
                bot_state['current_option_ltp'] = option_ltp
                try:
                    await self.check_trailing_sl(option_ltp)
                except Exception:
                    pass
        except Exception:
            pass

    def _simulate_option_ltp(self, index_name: str) -> None:
        """Compute synthetic option LTP for paper/SIM positions."""
        if not self.current_position:
            return
        security_id = self.current_position.get('security_id', '')
        if not security_id.startswith('SIM_'):
            return
        strike = self.current_position.get('strike', 0)
        option_type = self.current_position.get('option_type', '')
        index_ltp = bot_state['index_ltp']
        if not (strike and index_ltp):
            return
        distance_from_atm = abs(index_ltp - strike)
        if option_type == 'CE':
            intrinsic = max(0.0, index_ltp - strike)
        else:
            intrinsic = max(0.0, strike - index_ltp)
        time_value = 150 * max(0.0, 1 - distance_from_atm / 500)
        simulated_ltp = intrinsic + time_value + random.choice([-0.10, -0.05, 0, 0.05, 0.10])
        simulated_ltp = max(0.05, round(round(simulated_ltp / 0.05) * 0.05, 2))
        bot_state['current_option_ltp'] = simulated_ltp

    async def broadcast_state(self):
        """Broadcast current state to WebSocket clients"""
        from server import manager

        payload = {
            "type": "state_update",
            "data": {
                "index_ltp": bot_state['index_ltp'],
                "supertrend_signal": bot_state['last_supertrend_signal'],
                "supertrend_value": bot_state['supertrend_value'],
                "htf_supertrend_signal": bot_state.get('htf_supertrend_signal'),
                "htf_supertrend_value": bot_state.get('htf_supertrend_value', 0.0),
                "position": bot_state['current_position'],
                "entry_price": bot_state['entry_price'],
                "current_option_ltp": bot_state['current_option_ltp'],
                "trailing_sl": bot_state['trailing_sl'],
                "daily_pnl": bot_state['daily_pnl'],
                "daily_trades": bot_state['daily_trades'],
                "is_running": bot_state['is_running'],
                "bot_phase": state_machine.phase_name,
                "mode": bot_state['mode'],
                "mds_score": bot_state.get('mds_score', 0.0),
                "mds_slope": bot_state.get('mds_slope', 0.0),
                "mds_acceleration": bot_state.get('mds_acceleration', 0.0),
                "mds_stability": bot_state.get('mds_stability', 0.0),
                "mds_confidence": bot_state.get('mds_confidence', 0.0),
                "mds_is_choppy": bot_state.get('mds_is_choppy', False),
                "mds_direction": bot_state.get('mds_direction', 'NONE'),
                "trading_enabled": bool(config.get('trading_enabled', True)),
                "selected_index": config['selected_index'],
                "candle_interval": config['candle_interval'],
                "htf_filter_enabled": bool(config.get('htf_filter_enabled', True)),
                "htf_filter_timeframe": int(config.get('htf_filter_timeframe', 60)),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        }

        try:
            logger.debug(f"[STATE] Broadcasting state_update: index_ltp={payload['data'].get('index_ltp')} selected_index={payload['data'].get('selected_index')} mds_score={payload['data'].get('mds_score')}")
            await manager.broadcast(payload)
            logger.debug("[STATE] state_update broadcast complete")
        except Exception as e:
            logger.exception(f"[STATE] Failed to broadcast state_update: {e}")

    async def process_mds_on_close(self, mds_snapshot, index_ltp: float) -> bool:
        """Process score-engine snapshot on candle close.

        Entry/exit are driven purely by MDS + safety gates.
        """
        exited = False
        index_name = config['selected_index']
        index_config = get_index_config(index_name)

        runner = self._get_mds_runner()

        # Exit logic first
        if self.current_position:
            position_type = self.current_position.get('option_type', '')
            qty = int(self.current_position.get('qty') or 0)
            if qty <= 0:
                qty = int(config.get('order_qty', 1)) * index_config['lot_size']

            # Respect min-hold to avoid churn
            if self._min_hold_active():
                return False

            score = float(getattr(mds_snapshot, 'score', 0.0) or 0.0)
            slope = float(getattr(mds_snapshot, 'slope', 0.0) or 0.0)

            # Use slow timeframe MACD+Histogram as a confirmation signal for exits.
            # This reduces churn exits caused by base-TF noise.
            slow_mom = 0.0
            try:
                tf_scores = getattr(mds_snapshot, 'tf_scores', {}) or {}
                if isinstance(tf_scores, dict) and tf_scores:
                    slow_tf = max(int(k) for k in tf_scores.keys())
                    slow = tf_scores.get(slow_tf)
                    slow_mom = float(getattr(slow, 'macd_score', 0.0) or 0.0) + float(getattr(slow, 'hist_score', 0.0) or 0.0)
            except Exception:
                slow_mom = 0.0

            # NOTE: Momentum/score-based exits are disabled.
            # We rely only on fixed SL, target and trailing SL as exit mechanisms.
            # SL/target checks are handled by check_trailing_sl_on_close() and check_tick_sl().
            logger.debug("[MDS] Score/momentum exits disabled - SL/target handled by tick/close checks")

        # If a position is still open after exit checks, do not run entry logic
        if self.current_position:
            logger.info("[ENTRY_DECISION] NO | Reason=position_open (MDS)")
            return False

        # No position: entry logic

        # Enforce order cooldown between any two orders
        if not self._can_place_new_entry_order():
            remaining = self._remaining_entry_cooldown()
            logger.info(f"[MDS] Entry blocked - Order cooldown active ({remaining:.1f}s remaining)")
            return False

        if not can_take_new_trade():
            logger.info("[ENTRY_DECISION] NO | Reason=after_cutoff (MDS)")
            return False

        if bot_state['daily_trades'] >= config['max_trades_per_day']:
            logger.info(f"[MDS] Max daily trades reached ({config['max_trades_per_day']})")
            return False

        # Check min_trade_gap protection (optional)
        min_gap = config.get('min_trade_gap', 0)
        if min_gap > 0 and self.last_trade_time:
            time_since_last = (datetime.now() - self.last_trade_time).total_seconds()
            if time_since_last < min_gap:
                logger.info(f"[ENTRY_DECISION] NO | Reason=min_trade_gap (MDS) ({time_since_last:.1f}s < {min_gap}s)")
                return False

        direction = str(getattr(mds_snapshot, 'direction', 'NONE') or 'NONE')
        score = float(getattr(mds_snapshot, 'score', 0.0) or 0.0)
        slope = float(getattr(mds_snapshot, 'slope', 0.0) or 0.0)
        confidence = float(getattr(mds_snapshot, 'confidence', 0.0) or 0.0)

        ready = bool(getattr(mds_snapshot, 'ready', False))
        if not ready:
            logger.debug("[MDS] Skipping - Engine not ready yet (warming up)")
            return False

        is_choppy = bool(getattr(mds_snapshot, 'is_choppy', False))
        if is_choppy:
            logger.debug("[MDS] Skipping - Market is choppy")
            return False

        confirm_needed = 1 if bot_state.get('mode') == 'paper' else 2
        entry_decision = runner.decide_entry(
            ready=ready,
            is_choppy=is_choppy,
            direction=direction,
            score=score,
            slope=slope,
            confirm_needed=int(confirm_needed),
        )

        if not entry_decision.should_enter:
            if entry_decision.reason == 'neutral_band':
                logger.info(f"[ENTRY_DECISION] NO | Reason=neutral_band (MDS) | Score={score:.2f} Slope={slope:.2f}")
                return False
            if entry_decision.reason == 'score_too_low':
                logger.info(
                    f"[ENTRY_DECISION] NO | Reason=score_too_low (MDS) | Score={score:.2f} Slope={slope:.2f} Dir={direction}"
                )
                return False
            if entry_decision.reason == 'slope_too_low':
                logger.info(
                    f"[ENTRY_DECISION] NO | Reason=slope_too_low (MDS) | Score={score:.2f} Slope={slope:.2f} Dir={direction}"
                )
                return False
            if entry_decision.reason == 'arming':
                logger.info(
                    f"[MDS] Arming entry ({entry_decision.confirm_count}/{entry_decision.confirm_needed}) | "
                    f"Dir={direction} Score={score:.2f} Slope={slope:.2f} Conf={confidence:.2f}"
                )
            return False

        # Fixed lots: always use Settings value (order_qty). No confidence-based lot sizing.
        fixed_lots = int(config.get('order_qty', 1) or 1)

        option_type = entry_decision.option_type or ('CE' if direction == 'CE' else 'PE')
        atm_strike = round_to_strike(index_ltp, index_name)

        logger.info(
            f"[MDS] ENTRY | {option_type} | Score={score:.2f} Slope={slope:.2f} Conf={confidence:.2f} | "
            f"Lots={fixed_lots} | "
            f"Index={index_name} LTP={index_ltp:.2f} ATM={atm_strike}"
        )

        before = bot_state.get('current_position')
        await self.enter_position(option_type, atm_strike, index_ltp)
        after = bot_state.get('current_position')
        if before is None and after is not None:
            logger.info(f"[ENTRY_DECISION] YES | Confirmed (MDS) | {option_type} {atm_strike}")
        else:
            logger.info(f"[ENTRY_DECISION] NO | Entry blocked downstream (MDS) | {option_type} {atm_strike}")

        self.last_trade_time = datetime.now()
        runner.on_entry_attempted()
        return False
    
    async def check_trailing_sl(self, current_ltp: float):
        """Update SL values - initial fixed SL then trails profit using step-based method"""
        if not self.current_position:
            return

        # Coerce config values to numbers and validate
        try:
            trail_start = float(config.get('trail_start_profit', 0) or 0)
        except Exception:
            trail_start = 0.0
        try:
            trail_step = float(config.get('trail_step', 0) or 0)
        except Exception:
            trail_step = 0.0

        if trail_start <= 0 or trail_step <= 0:
            # Trailing disabled - don't set any SL
            return

        try:
            profit_points = float(current_ltp) - float(self.entry_price)
        except Exception:
            return

        # Track highest profit reached (use max to avoid regressions)
        self.highest_profit = max(self.highest_profit, profit_points)

        # Diagnostic log to help debug trailing behavior (DEBUG level to avoid log flood)
        logger.debug(
            f"[SL] check called | LTP={current_ltp:.2f} Entry={self.entry_price:.2f} "
            f"Profit={profit_points:.2f} Highest={self.highest_profit:.2f} "
            f"TrailStart={trail_start} Step={trail_step} CurrentTSL={self.trailing_sl}"
        )

        # Step 1: Set initial fixed stoploss (if enabled)
        try:
            initial_sl = float(config.get('initial_stoploss', 0) or 0)
        except Exception:
            initial_sl = 0.0

        if initial_sl > 0 and self.trailing_sl is None:
            self.trailing_sl = float(self.entry_price) - initial_sl
            bot_state['trailing_sl'] = self.trailing_sl
            logger.info(f"[SL] Initial SL set: {self.trailing_sl:.2f} ({initial_sl} pts below entry)")
            logger.debug(f"[SL] Continuing after initial SL; current_ltp={current_ltp:.2f} entry={self.entry_price:.2f} profit_points={profit_points:.2f}")

        # Step 2: Activate trailing once highest profit exceeds trail_start
        if self.highest_profit < trail_start:
            return

        # Compute how many full steps we've moved past trail_start
        trail_levels = int(math.floor((self.highest_profit - trail_start) / trail_step))
        locked_profit = (trail_start - trail_step) + (trail_levels * trail_step)
        locked_profit = max(0.0, locked_profit)
        new_sl = float(self.entry_price) + locked_profit

        # Always move SL up, never down (protect profit)
        if self.trailing_sl is None or new_sl > float(self.trailing_sl):
            old_sl = self.trailing_sl
            self.trailing_sl = new_sl
            bot_state['trailing_sl'] = self.trailing_sl

            logger.debug(f"[SL] Computed new_sl={new_sl:.2f} locked_profit={locked_profit:.2f} highest_profit={self.highest_profit:.2f} trail_start={trail_start} trail_step={trail_step}")
            if old_sl is not None and old_sl > (float(self.entry_price) - initial_sl):
                logger.info(f"[SL] Trailing SL updated: {old_sl:.2f} → {new_sl:.2f} (Profit: {profit_points:.2f} pts)")
            else:
                logger.info(f"[SL] Trailing started: {new_sl:.2f} (Profit: {profit_points:.2f} pts)")

    
    async def check_trailing_sl_on_close(self, current_ltp: float) -> bool:
        """Check if trailing SL or target is hit on candle close"""
        if not self.current_position:
            return False
        
        index_config = get_index_config(config['selected_index'])
        qty = int(self.current_position.get('qty') or 0)
        if qty <= 0:
            qty = config['order_qty'] * index_config['lot_size']
        profit_points = current_ltp - self.entry_price
        
        # Only fixed SL and target exits
        target_points = config.get('target_points', 0)
        sl_points = config.get('initial_stoploss', 0)
        # Target exit
        if target_points > 0 and profit_points >= target_points:
            pnl = profit_points * qty
            logger.info(
                f"[EXIT] Target hit | LTP={current_ltp:.2f} | Entry={self.entry_price:.2f} | Profit={profit_points:.2f} pts | Target={target_points:.2f} pts"
            )
            closed = await self.close_position(current_ltp, pnl, "Target Hit")
            return bool(closed)
        # Fixed SL exit
        if sl_points > 0 and profit_points <= -sl_points:
            pnl = profit_points * qty
            logger.info(
                f"[EXIT] Stop-loss hit | LTP={current_ltp:.2f} | Entry={self.entry_price:.2f} | Loss={profit_points:.2f} pts | SL={sl_points:.2f} pts"
            )
            closed = await self.close_position(current_ltp, pnl, "Stop-loss Hit")
            return bool(closed)
        # Trailing SL exit (absolute price stored in self.trailing_sl)
        try:
            tsl = getattr(self, 'trailing_sl', None)
        except Exception:
            tsl = None
        if tsl is not None:
            # Exit when price falls to or below trailing SL
            if current_ltp <= tsl:
                pnl = profit_points * qty
                logger.info(
                    f"[EXIT] Trailing stop hit (close) | LTP={current_ltp:.2f} | Entry={self.entry_price:.2f} | SL={tsl:.2f}"
                )
                closed = await self.close_position(current_ltp, pnl, "Trailing SL Hit")
                return bool(closed)
        # Max trade duration (candle-close enforcement)
        try:
            max_dur = int(config.get('max_trade_duration_seconds', 0) or 0)
        except Exception:
            max_dur = 0
        if max_dur > 0 and self.entry_time_utc:
            now = datetime.now(timezone.utc)
            elapsed = (now - self.entry_time_utc).total_seconds()
            if elapsed >= max_dur:
                pnl = profit_points * qty
                logger.info(
                    f"[EXIT] Max duration hit (close) | Elapsed={elapsed:.1f}s | LTP={current_ltp:.2f} | Entry={self.entry_price:.2f} | MaxDur={max_dur}s"
                )
                closed = await self.close_position(current_ltp, pnl, "Max Duration Hit")
                return bool(closed)
        return False
    
    async def check_tick_sl(self, current_ltp: float) -> bool:
        """Check SL/Target on every tick (more responsive than candle close)"""
        if not self.current_position:
            return False
        if not state_machine.can_exit:
            return False
        
        index_config = get_index_config(config['selected_index'])
        qty = int(self.current_position.get('qty') or 0)
        if qty <= 0:
            qty = config['order_qty'] * index_config['lot_size']
        profit_points = current_ltp - self.entry_price
        pnl = profit_points * qty
        
        # Only fixed SL and target exits
        target_points = config.get('target_points', 0)
        sl_points = config.get('initial_stoploss', 0)
        # Target exit
        if target_points > 0 and profit_points >= target_points:
            logger.info(
                f"[EXIT] Target hit (tick) | LTP={current_ltp:.2f} | Entry={self.entry_price:.2f} | Profit={profit_points:.2f} pts | Target={target_points:.2f} pts"
            )
            closed = await self.close_position(current_ltp, pnl, "Target Hit")
            return bool(closed)
        # Fixed SL exit
        if sl_points > 0 and profit_points <= -sl_points:
            logger.info(
                f"[EXIT] Stop-loss hit (tick) | LTP={current_ltp:.2f} | Entry={self.entry_price:.2f} | Loss={profit_points:.2f} pts | SL={sl_points:.2f} pts"
            )
            closed = await self.close_position(current_ltp, pnl, "Stop-loss Hit")
            return bool(closed)
        # Trailing SL exit (tick-level)
        try:
            tsl = getattr(self, 'trailing_sl', None)
        except Exception:
            tsl = None
        if tsl is not None:
            if current_ltp <= tsl:
                logger.info(
                    f"[EXIT] Trailing stop hit (tick) | LTP={current_ltp:.2f} | Entry={self.entry_price:.2f} | SL={tsl:.2f}"
                )
                closed = await self.close_position(current_ltp, pnl, "Trailing SL Hit")
                return bool(closed)
        # Max trade duration (tick-level enforcement)
        try:
            max_dur = int(config.get('max_trade_duration_seconds', 0) or 0)
        except Exception:
            max_dur = 0
        if max_dur > 0 and self.entry_time_utc:
            now = datetime.now(timezone.utc)
            elapsed = (now - self.entry_time_utc).total_seconds()
            if elapsed >= max_dur:
                logger.info(
                    f"[EXIT] Max duration hit (tick) | Elapsed={elapsed:.1f}s | LTP={current_ltp:.2f} | Entry={self.entry_price:.2f} | MaxDur={max_dur}s"
                )
                closed = await self.close_position(current_ltp, pnl, "Max Duration Hit")
                return bool(closed)
        return False
    
    async def process_signal_on_close(self, signal: str, index_ltp: float, flipped: bool = False) -> bool:
        """Process SuperTrend signal on candle close"""
        # SuperTrend-based signal processing is removed; when using ScoreMds
        # the ScoreMdsRuntime will call `process_mds_on_close` instead.
        indicator_type = str(config.get('indicator_type', 'score_mds') or '').strip().lower()
        if indicator_type == 'score_mds':
            return False

        exited = False
        index_name = config['selected_index']
        index_config = get_index_config(index_name)
        qty = 0
        if self.current_position:
            qty = int(self.current_position.get('qty') or 0)
        if qty <= 0:
            qty = config['order_qty'] * index_config['lot_size']
        
        runner = self._get_st_runner()

        # Remove only momentum/score exits, keep trailing SL/target exits
        if self.current_position:
            logger.info("[ENTRY_DECISION] NO | Reason=position_open")
            return False

        # Enforce order cooldown between any two orders (prevents exit->entry flip too fast)
        if not self._can_place_new_entry_order():
            remaining = self._remaining_entry_cooldown()
            logger.info(f"[ENTRY] ✗ Skipping - Order cooldown active ({remaining:.1f}s remaining)")
            logger.info("[ENTRY_DECISION] NO | Reason=order_cooldown")
            return exited
        
        if not can_take_new_trade():
            logger.info("[ENTRY_DECISION] NO | Reason=after_cutoff")
            return exited
        
        if bot_state['daily_trades'] >= config['max_trades_per_day']:
            logger.info(f"[SIGNAL] Max daily trades reached ({config['max_trades_per_day']})")
            logger.info("[ENTRY_DECISION] NO | Reason=max_daily_trades")
            return exited
        
        # Check min_trade_gap protection (optional)
        min_gap = config.get('min_trade_gap', 0)
        if min_gap > 0 and self.last_trade_time:
            time_since_last = (datetime.now() - self.last_trade_time).total_seconds()
            if time_since_last < min_gap:
                logger.info(f"[ENTRY_DECISION] NO | Reason=min_trade_gap ({time_since_last:.1f}s < {min_gap}s)")
                return exited
        
        # Enter new position
        if not signal:
            logger.info("[ENTRY_DECISION] NO | Reason=no_signal")
            return exited

        adx_last = None
        try:
            if self.adx and getattr(self.adx, 'adx_values', None):
                adx_last = float(self.adx.adx_values[-1])
        except Exception:
            adx_last = None

        entry_decision = runner.decide_entry(
            signal=signal,
            flipped=bool(flipped),
            trade_only_on_flip=bool(config.get('trade_only_on_flip', False)),
            htf_filter_enabled=bool(config.get('htf_filter_enabled', True)),
            candle_interval_seconds=int(config.get('candle_interval', 60) or 60),
            htf_direction=int(getattr(self.htf_indicator, 'direction', 0) if self.htf_indicator else 0),
            macd_confirmation_enabled=bool(config.get('macd_confirmation_enabled', True)),
            macd_last=(float(self.macd.last_macd) if (self.macd and self.macd.last_macd is not None) else None),
            macd_signal_line=(float(self.macd.last_signal_line) if (self.macd and self.macd.last_signal_line is not None) else None),
            adx_value=adx_last,
            adx_threshold=float(config.get('adx_threshold', 25.0) or 25.0),
        )

        if not entry_decision.should_enter:
            self._log_st_entry_block(reason=entry_decision.reason, signal=signal, flipped=bool(flipped))
            return exited
        
        # NOTE: Previously we compared against last trade signal (self.last_signal).
        # That behavior is replaced by candle-level flip detection via the `flipped` flag.
        
        option_type = str(entry_decision.option_type or '').strip().upper() or ('PE' if signal == 'RED' else 'CE')
        atm_strike = round_to_strike(index_ltp, index_name)
        
        # Log signal details
        logger.info(
            f"[ENTRY] Taking {option_type} | {signal} Signal | "
            f"Index: {index_name} | "
            f"LTP: {index_ltp:.2f} | "
            f"ATM Strike: {atm_strike} | "
            f"SuperTrend: {bot_state['supertrend_value']:.2f}"
        )

        before = bot_state.get('current_position')
        await self.enter_position(option_type, atm_strike, index_ltp)
        after = bot_state.get('current_position')
        if before is None and after is not None:
            logger.info(f"[ENTRY_DECISION] YES | Confirmed | {option_type} {atm_strike}")
        else:
            logger.info(f"[ENTRY_DECISION] NO | Entry blocked downstream | {option_type} {atm_strike}")
        self.last_trade_time = datetime.now()
        
        return exited
    
    async def enter_position(self, option_type: str, strike: int, index_ltp: float, override_lots: int | None = None):
        """Enter a new position with market validation"""
        # Soft pause: keep bot running (prices/indicators/exits), but block new entries
        if not config.get('trading_enabled', True):
            now = datetime.now(timezone.utc)
            if (
                self._last_entries_paused_log_time is None
                or (now - self._last_entries_paused_log_time).total_seconds() >= 10
            ):
                logger.info(
                    f"[ENTRY] Skipped - Trading paused (trading_enabled=false) | Would take {option_type} {strike}"
                )
                self._last_entries_paused_log_time = now
            return

        # CRITICAL: Double-check market is open before entering
        # Skip this check for paper replay mode or when bypass_market_hours is enabled
        if not (
            (bot_state.get('mode') == 'paper' and config.get('paper_replay_enabled', False))
            or config.get('bypass_market_hours', False)
        ):
            if not is_market_open():
                logger.warning(f"[ENTRY] ✗ BLOCKED - Market is CLOSED | Cannot enter {option_type} position")
                return
        
        # CHECK: Trading hours protection
        if not self.is_within_trading_hours():
            logger.warning(f"[ENTRY] ✗ BLOCKED - Outside trading hours | Cannot enter {option_type} position")
            return

        # Enforce minimum cooldown between orders
        if not self._can_place_new_entry_order():
            remaining = self._remaining_entry_cooldown()
            logger.info(f"[ENTRY] ✗ BLOCKED - Order cooldown active ({remaining:.1f}s remaining)")
            return
        
        index_name = config['selected_index']
        index_config = get_index_config(index_name)

        lots = int(config.get('order_qty', 1) or 1)
        if override_lots is not None:
            lots = max(1, int(override_lots))

        # Fixed lots by default (order_qty). Risk-based lot reduction is opt-in.
        qty = lots * index_config['lot_size']

        if bool(config.get('enable_risk_based_lots', False)):
            risk_per_trade = float(config.get('risk_per_trade', 0) or 0)
            sl_points = float(config.get('initial_stoploss', 0) or 0)
            if risk_per_trade > 0 and sl_points > 0:
                max_lots = int(risk_per_trade / (sl_points * index_config['lot_size']))
                if max_lots < 1:
                    logger.warning(
                        f"[POSITION] ✗ BLOCKED - risk_per_trade too low for 1 lot | Risk=₹{risk_per_trade} SL={sl_points} LotSize={index_config['lot_size']}"
                    )
                    return
                new_lots = max(1, min(int(max_lots), int(lots)))
                if new_lots != lots:
                    lots = new_lots
                    qty = lots * index_config['lot_size']
                    logger.info(
                        f"[POSITION] Size adjusted for risk: {lots} lots ({qty} qty) (Risk: ₹{risk_per_trade}, SL: {sl_points}pts)"
                    )
        
        trade_id = f"T{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Get expiry
        expiry = await self.dhan.get_nearest_expiry(index_name) if self.dhan else None

        entry_price = 0.0
        security_id = None

        # PAPER MODE
        if bot_state['mode'] == 'paper':
            used_live_quote = False

            try:
                if self._paper_should_use_live_option_quotes() and self.dhan:
                    security_id = await self.dhan.get_atm_option_security_id(
                        index_name, strike, option_type, expiry
                    )

                    if security_id:
                        option_ltp = await self.dhan.get_option_ltp(
                            security_id=security_id,
                            strike=strike,
                            option_type=option_type,
                            expiry=expiry,
                            index_name=index_name
                        )

                        if option_ltp and float(option_ltp) > 0:
                            entry_price = round(float(option_ltp) / 0.05) * 0.05
                            entry_price = round(entry_price, 2)
                            used_live_quote = True
                            logger.debug(f"[ENTRY] PAPER live-quote fetched: SecID={security_id} LTP={entry_price} Strike={strike} OptType={option_type} Index={index_name}")

            except Exception as e:
                logger.debug(f"[ENTRY] PAPER live-quote failed: {e}")

            # Fallback to synthetic pricing
            if not used_live_quote:
                security_id = f"SIM_{index_name}_{strike}_{option_type}"

                distance = abs(index_ltp - strike)
                intrinsic = max(0, index_ltp - strike) if option_type == 'CE' else max(0, strike - index_ltp)
                time_value = 150 * max(0, 1 - (distance / 500))

                entry_price = intrinsic + time_value
                entry_price = round(entry_price / 0.05) * 0.05
                entry_price = round(entry_price, 2)

            label = "PAPER(LIVE-QUOTE)" if used_live_quote else "PAPER(SYNTHETIC)"
            logger.info(
                f"[ENTRY] {label} | {index_name} {option_type} {strike} | "
                f"Expiry: {expiry} | Price: {entry_price} | Qty: {qty} | SecID: {security_id}"
            )
        
        # Live mode
        else:
            if not self.dhan:
                # Mode can be switched to live while the bot is running.
                # Try a lazy init once before failing the trade.
                try:
                    if not self.initialize_dhan():
                        logger.error("[ERROR] Dhan API not initialized")
                        return
                except Exception:
                    logger.error("[ERROR] Dhan API not initialized")
                    return

            # Resolve instrument + a more accurate entry price from market data
            try:
                security_id = await self.dhan.get_atm_option_security_id(index_name, strike, option_type, expiry)
                if not security_id:
                    logger.error(f"[ERROR] Could not find security ID for {index_name} {strike} {option_type}")
                    return
                logger.debug(f"[ENTRY] LIVE resolved SecID={security_id} Strike={strike} OptType={option_type} Index={index_name}")
                option_ltp = await self.dhan.get_option_ltp(
                    security_id=security_id,
                    strike=strike,
                    option_type=option_type,
                    expiry=expiry,
                    index_name=index_name
                )
                if option_ltp > 0:
                    entry_price = round(option_ltp / 0.05) * 0.05
                    entry_price = round(entry_price, 2)
            except Exception as e:
                logger.error("[ERROR] Failed to get entry price: %s", e)
                return
            
            result = await self.dhan.place_order(security_id, "BUY", qty, index_name=index_name)
            logger.info(f"[ORDER] Entry order result: {result}")

            # Check if order was successfully placed
            if result.get('status') != 'success' or not result.get('orderId'):
                logger.error(f"[ERROR] Failed to place entry order: {result}")
                state_machine.entry_failed()
                return

            state_machine.placing_entry()
            # Order placed successfully - save to DB immediately
            order_id = result.get('orderId')

            # Track last order timestamp (entry order) right after placing the order
            self.last_order_time_utc = datetime.now(timezone.utc)

            verify = await self.dhan.verify_order_filled(
                order_id=str(order_id),
                security_id=str(security_id),
                expected_qty=int(qty),
                timeout_seconds=30
            )
            if not verify.get('filled'):
                logger.error(
                    f"[ORDER] ✗ Entry order NOT filled | OrderID: {order_id} | Status: {verify.get('status')} | {verify.get('message')}"
                )
                return

            avg_price = float(verify.get('average_price') or 0)
            if avg_price > 0:
                entry_price = round(avg_price / 0.05) * 0.05
                entry_price = round(entry_price, 2)
                logger.debug(f"[ENTRY] LIVE order fill average price: OrderID={order_id} FillPrice={entry_price} SecID={security_id}")
            
            logger.info(
                f"[ENTRY] LIVE | {index_name} {option_type} {strike} | Expiry: {expiry} | OrderID: {order_id} | Fill Price: {entry_price} | Qty: {qty}"
            )

        # Track last order timestamp (paper mode entry)
        if bot_state['mode'] == 'paper':
            self.last_order_time_utc = datetime.now(timezone.utc)

        # Guard: never open a position with zero entry price (corrupts SL calculations)
        if entry_price <= 0:
            logger.error(f"[ENTRY] ✗ BLOCKED - entry_price is 0 or negative ({entry_price}) | {index_name} {option_type} {strike}")
            return

        # Save position
        self.current_position = {
            'trade_id': trade_id,
            'option_type': option_type,
            'strike': strike,
            'expiry': expiry,
            'security_id': security_id,
            'index_name': index_name,
            'qty': qty,
            'entry_time': datetime.now(timezone.utc).isoformat()
        }
        self.entry_price = entry_price
        self.trailing_sl = None
        self.highest_profit = 0
        self.entry_time_utc = datetime.now(timezone.utc)

        state_machine.entry_confirmed()

        bot_state['current_position'] = self.current_position
        bot_state['entry_price'] = self.entry_price
        bot_state['daily_trades'] += 1
        bot_state['current_option_ltp'] = entry_price
        logger.debug(f"[ENTRY] setting current_option_ltp to entry_price: TradeID={trade_id} EntryPrice={entry_price} Mode={bot_state['mode']}")
        try:
            await self.check_trailing_sl(bot_state['current_option_ltp'])
        except Exception:
            logger.debug("[SL] check_trailing_sl failed after setting entry price")

        # Start a background monitor to ensure trailing SL updates on any LTP changes
        try:
            # Cancel any existing trailing task (safety)
            if getattr(self, '_trailing_task', None) is not None:
                try:
                    self._trailing_task.cancel()
                except Exception:
                    pass

            async def _trailing_monitor():
                try:
                    logger.info(f"[SL] Trailing monitor started for TradeID={trade_id}")
                    while self.current_position is not None:
                        try:
                            ltp = bot_state.get('current_option_ltp') or 0.0
                            await self.check_trailing_sl(float(ltp))
                            # Enforce max trade duration from the monitor as well
                            try:
                                max_dur = int(config.get('max_trade_duration_seconds', 0) or 0)
                            except Exception:
                                max_dur = 0
                            if max_dur > 0 and self.entry_time_utc:
                                now = datetime.now(timezone.utc)
                                elapsed = (now - self.entry_time_utc).total_seconds()
                                if elapsed >= max_dur:
                                    # Compute qty and pnl similar to squareoff
                                    index_name = self.current_position.get('index_name', config['selected_index'])
                                    index_config = get_index_config(index_name)
                                    qty = int(self.current_position.get('qty') or 0)
                                    if qty <= 0:
                                        qty = config['order_qty'] * index_config['lot_size']
                                    exit_price = float(ltp or bot_state.get('current_option_ltp') or 0.0)
                                    pnl = (exit_price - self.entry_price) * qty
                                    logger.info(
                                        f"[EXIT] Max duration hit (monitor) | Elapsed={elapsed:.1f}s | LTP={exit_price:.2f} | Entry={self.entry_price:.2f} | MaxDur={max_dur}s"
                                    )
                                    try:
                                        closed = await self.close_position(exit_price, pnl, "Max Duration Hit")
                                    except Exception:
                                        closed = False
                                    if closed:
                                        break
                        except asyncio.CancelledError:
                            break
                        except Exception:
                            pass
                        await asyncio.sleep(1)
                except asyncio.CancelledError:
                    pass
                finally:
                    logger.info(f"[SL] Trailing monitor stopped for TradeID={trade_id}")

            self._trailing_task = asyncio.create_task(_trailing_monitor())
        except Exception:
            logger.debug("[SL] Failed to start trailing monitor task")
        
        # ONLY set last_signal AFTER position is successfully confirmed open
        self.last_signal = option_type[0].upper() + 'E'  # 'CE' -> 'C', 'PE' -> 'P'
        self.last_signal = 'GREEN' if option_type == 'CE' else 'RED'
        
        # Save to database in background - don't wait for DB commit
        asyncio.create_task(save_trade({
            'trade_id': trade_id,
            'entry_time': datetime.now(timezone.utc).isoformat(),
            'option_type': option_type,
            'strike': strike,
            'expiry': expiry,
            'entry_price': self.entry_price,
            'qty': qty,
            'mode': bot_state['mode'],
            'index_name': index_name,
            'created_at': datetime.now(timezone.utc).isoformat()
        }))

