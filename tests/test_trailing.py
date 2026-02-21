import asyncio
import sys
import os

# Ensure backend dir is importable and its modules (config, trading_bot) resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))
import types

# Provide a fake lightweight `database` module to avoid heavy dependencies during unit test
fake_db = types.ModuleType('database')
async def _fake_save_trade(*args, **kwargs):
    return True
async def _fake_update_trade_exit(*args, **kwargs):
    return True
fake_db.save_trade = _fake_save_trade
fake_db.update_trade_exit = _fake_update_trade_exit
import sys as _sys
_sys.modules['database'] = fake_db

import trading_bot
from trading_bot import TradingBot
import config as cfgmod
bot_state = cfgmod.bot_state


async def run_test():
    # Prepare bot and config
    cfgmod.config['trail_start_profit'] = 10.0
    cfgmod.config['trail_step'] = 5.0
    cfgmod.config['initial_stoploss'] = 10.0

    bot = TradingBot()

    # Simulate an open paper position
    bot.current_position = {
        'trade_id': 'TTEST',
        'option_type': 'CE',
        'strike': 25000,
        'qty': 1,
    }
    bot.entry_price = 100.0
    bot.trailing_sl = None
    bot.highest_profit = 0.0

    ltps = [100.0, 103.0, 106.0, 111.0, 116.0, 120.0]

    for ltp in ltps:
        print(f"Calling check_trailing_sl with LTP={ltp}")
        await bot.check_trailing_sl(ltp)
        print(f"-> trailing_sl={bot.trailing_sl} highest_profit={bot.highest_profit}\n")


if __name__ == '__main__':
    asyncio.run(run_test())
