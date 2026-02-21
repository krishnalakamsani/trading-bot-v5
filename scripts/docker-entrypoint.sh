#!/bin/sh
set -e

# Default URI points to the Compose service name so it resolves inside the network
URI=${SENDER_URI:-ws://backend:8001/ws}
ARGS=""

# Attach token as query param if provided
if [ -n "$SENDER_TOKEN" ]; then
  if echo "$URI" | grep -q '\?'; then
    URI="$URI&token=$SENDER_TOKEN"
  else
    URI="$URI?token=$SENDER_TOKEN"
  fi
fi

if [ -n "$SENDER_LTP" ]; then
  ARGS="$ARGS --ltp $SENDER_LTP"
fi

if [ -n "$SENDER_INTERVAL" ]; then
  ARGS="$ARGS --interval $SENDER_INTERVAL"
fi

if [ "$SENDER_ONCE" = "1" ] || [ "$SENDER_ONCE" = "true" ]; then
  ARGS="$ARGS --once"
fi

if [ "$SENDER_JITTER" = "1" ] || [ "$SENDER_JITTER" = "true" ]; then
  ARGS="$ARGS --jitter"
fi

echo "Starting ws_reconnect_sender with: uri=$URI $ARGS"
exec python3 /app/ws_reconnect_sender.py --uri "$URI" $ARGS
