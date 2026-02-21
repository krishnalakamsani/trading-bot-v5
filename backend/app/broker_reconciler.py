"""BrokerReconciler — syncs bot state with actual Dhan positions on startup.

Called once when the bot starts in live mode, before the run_loop begins.

Scenarios it handles:
  1. Bot crashed while flat → no positions in Dhan → nothing to do
  2. Bot crashed during entry → Dhan has a position → rebuild current_position,
     fetch live LTP, resume monitoring
  3. Bot crashed during exit → Dhan still has a position → resume monitoring;
     exit will re-attempt on next SL/signal check

What it does NOT do:
  - Place any orders
  - Change risk config
  - Run in paper mode (paper is always synthetic)
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


async def reconcile_with_broker(bot) -> bool:
    """Check Dhan positions and rebuild bot state if an open position is found.

    Args:
        bot: TradingBot instance (must have .dhan initialised)

    Returns:
        True if a position was found and state rebuilt, False if flat.
    """
    from config import bot_state, config
    from indices import get_index_config

    if not bot.dhan:
        logger.info("[RECONCILE] Skipped — Dhan not initialised (paper mode or no credentials)")
        return False

    logger.info("[RECONCILE] Checking Dhan for open positions...")

    try:
        positions = await asyncio.to_thread(bot.dhan.dhan.get_positions)
        raw = []
        if isinstance(positions, dict):
            raw = positions.get('data', []) or []
        elif isinstance(positions, list):
            raw = positions
    except Exception as e:
        logger.warning(f"[RECONCILE] Failed to fetch positions from Dhan: {e}")
        return False

    # Filter to net-open option positions (qty != 0)
    open_positions = [
        p for p in raw
        if isinstance(p, dict)
        and int(p.get('netQty') or p.get('qty') or 0) != 0
        and str(p.get('productType') or '').upper() in ('INTRADAY', 'CNC', 'MARGIN', 'MTF', '')
    ]

    if not open_positions:
        logger.info("[RECONCILE] No open positions found in Dhan — bot is flat")
        return False

    if len(open_positions) > 1:
        logger.warning(
            f"[RECONCILE] {len(open_positions)} open positions found — expected at most 1. "
            "Using the first one. Review manually."
        )

    pos = open_positions[0]
    logger.info(f"[RECONCILE] Open position found: {pos}")

    # Extract fields from Dhan position dict
    try:
        security_id  = str(pos.get('securityId') or pos.get('security_id') or '')
        trading_sym  = str(pos.get('tradingSymbol') or pos.get('trading_symbol') or '')
        qty          = abs(int(pos.get('netQty') or pos.get('qty') or 0))
        avg_price    = float(pos.get('buyAvg') or pos.get('costPrice') or pos.get('avgPrice') or 0.0)
        exchange_seg = str(pos.get('exchangeSegment') or 'NSE_FNO')
    except Exception as e:
        logger.error(f"[RECONCILE] Failed to parse position dict: {e}")
        return False

    if not security_id or qty == 0:
        logger.warning("[RECONCILE] Position missing securityId or qty — skipping")
        return False

    # Infer option_type from trading symbol (e.g. NIFTY24JAN23000CE)
    option_type = 'CE' if trading_sym.endswith('CE') else 'PE' if trading_sym.endswith('PE') else ''

    # Infer index from trading symbol
    index_name = config.get('selected_index', 'NIFTY')
    for idx in ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'SENSEX', 'MIDCPNIFTY'):
        if trading_sym.upper().startswith(idx):
            index_name = idx
            break

    # Try to parse strike from symbol (last digits before CE/PE)
    strike = 0
    try:
        suffix = trading_sym[-2:]  # CE or PE
        digits = ''.join(c for c in trading_sym[:-2] if c.isdigit())[-6:]
        strike = int(digits) if digits else 0
    except Exception:
        pass

    # Fetch live LTP for the option
    live_ltp = avg_price  # fallback to avg cost
    try:
        live_ltp = await asyncio.to_thread(
            bot.dhan.get_index_and_option_ltp, index_name, int(security_id)
        )
        if isinstance(live_ltp, tuple):
            live_ltp = live_ltp[1] or avg_price
        live_ltp = float(live_ltp) if live_ltp and float(live_ltp) > 0 else avg_price
    except Exception as e:
        logger.warning(f"[RECONCILE] Could not fetch live LTP, using avg cost: {e}")

    trade_id = f"RECONCILE_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    # Rebuild bot state
    bot.current_position = {
        'trade_id':    trade_id,
        'option_type': option_type,
        'strike':      strike,
        'expiry':      '',          # not critical for monitoring
        'security_id': security_id,
        'index_name':  index_name,
        'qty':         qty,
        'entry_time':  datetime.now(timezone.utc).isoformat(),
    }
    bot.entry_price       = avg_price
    bot.trailing_sl       = None
    bot.highest_profit    = 0.0
    bot.entry_time_utc    = datetime.now(timezone.utc)

    bot_state['current_position']   = bot.current_position
    bot_state['entry_price']        = avg_price
    bot_state['current_option_ltp'] = live_ltp
    bot_state['trailing_sl']        = None

    logger.info(
        f"[RECONCILE] ✓ State rebuilt | {index_name} {option_type} {strike} | "
        f"Qty={qty} | AvgCost=₹{avg_price:.2f} | LiveLTP=₹{live_ltp:.2f} | "
        f"SecurityID={security_id}"
    )
    return True
