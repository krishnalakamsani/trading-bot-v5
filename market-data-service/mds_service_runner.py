import asyncio
import signal
import logging
import os

from config import config

logger = logging.getLogger(__name__)


async def _run():
    try:
        from dhan_api import DhanAPI
        from market_data_service import MarketDataService
    except Exception as e:
        logger.exception(f"Failed to import market data modules: {e}")
        return

    # Debug: log what we see from config and environment (helps diagnose missing creds)
    logger.info(f"config.dhan_access_token={config.get('dhan_access_token')!r}, config.dhan_client_id={config.get('dhan_client_id')!r}")
    logger.info(f"env DHAN_ACCESS_TOKEN present={bool(os.getenv('DHAN_ACCESS_TOKEN'))}, DHAN_CLIENT_ID present={bool(os.getenv('DHAN_CLIENT_ID'))}")

    # Prefer credentials from runtime config, fall back to environment variables
    dhan_access = config.get('dhan_access_token') or os.getenv('DHAN_ACCESS_TOKEN')
    dhan_client = config.get('dhan_client_id') or os.getenv('DHAN_CLIENT_ID')

    if not dhan_access or not dhan_client:
        logger.error("Dhan credentials not configured; cannot start MarketDataService")
        return

    dhan = DhanAPI(dhan_access, dhan_client)
    mds = MarketDataService(dhan)

    stop_event = asyncio.Event()

    def _on_signal(signum, frame=None):
        logger.info(f"Received signal {signum}; stopping MarketDataService")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, lambda s=s: _on_signal(s))

    await mds.start()

    try:
        await stop_event.wait()
    finally:
        await mds.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_run())


if __name__ == '__main__':
    main()
