#!/usr/bin/env python3
# Reconnecting WebSocket tick sender for niftyalgo backend
#
# Usage:
#     python3 ws_reconnect_sender.py --uri ws://localhost:8001/ws --ltp 23550.0 --interval 1.0
#
# Features:
# - Reconnects with exponential backoff
# - Sends JSON ticks of the form {"index_ltp": <float>} or {"type":"tick","data":{"index_ltp":...}}
# - Optional continuous sending at given interval
import argparse
import asyncio
import json
import random
import sys

try:
    import websockets
except Exception:
    print("websockets package not found. Install with: pip install websockets", file=sys.stderr)
    raise


async def send_loop(uri: str, ltp: float | None, interval: float, once: bool, jitter: bool):
    backoff = 0.5
    max_backoff = 30.0
    while True:
        try:
            async with websockets.connect(uri) as ws:
                print(f"Connected to {uri}")
                backoff = 0.5

                if once:
                    payload = {"index_ltp": float(ltp) if ltp is not None else random.uniform(20000, 30000)}
                    await ws.send(json.dumps(payload))
                    print("Sent:", payload)

                    # Wait for server ACK so one-shot senders don't close before broadcast.
                    try:
                        ack_wait = 5.0
                        resp = await asyncio.wait_for(ws.recv(), timeout=ack_wait)
                        try:
                            j = json.loads(resp)
                            print("Received:", j)
                        except Exception:
                            print("Received (non-JSON):", resp)
                    except asyncio.TimeoutError:
                        print(f"No ACK received within {ack_wait}s; exiting")
                    except Exception as e:
                        print("Error while waiting for ACK:", e)

                    return

                while True:
                    value = float(ltp) if ltp is not None else round(random.uniform(23000, 24000), 2)
                    payload = {"index_ltp": value}
                    try:
                        await ws.send(json.dumps(payload))
                        print("Sent:", payload)
                    except Exception as e:
                        print("Send failed:", e)
                        raise

                    # optional jitter to avoid perfect periodic bursts
                    sleep_t = interval + (random.random() * 0.1 if jitter else 0.0)
                    await asyncio.sleep(sleep_t)

        except (asyncio.CancelledError, KeyboardInterrupt):
            raise
        except Exception as e:
            print(f"Connection error: {e}; reconnecting in {backoff:.1f}s")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)


def main():
    p = argparse.ArgumentParser(description="Reconnecting WS tick sender for niftyalgo backend")
    p.add_argument("--uri", default="ws://localhost:8001/ws", help="WebSocket URI")
    p.add_argument("--ltp", type=float, default=None, help="Fixed LTP to send (default: random)")
    p.add_argument("--interval", type=float, default=1.0, help="Interval seconds between ticks (default 1.0)")
    p.add_argument("--once", action="store_true", help="Send one tick and exit")
    p.add_argument("--jitter", action="store_true", help="Add small jitter to interval to avoid bursts")

    args = p.parse_args()

    try:
        asyncio.run(send_loop(args.uri, args.ltp, args.interval, args.once, args.jitter))
    except KeyboardInterrupt:
        print("Interrupted")


if __name__ == "__main__":
    main()
