#!/bin/sh
# Start the pstack flow dashboard and open it in the browser.
# Usage: ./run.sh [port]   (default 8765). Ctrl-C to stop.
PORT="${1:-8765}"
cd "$(dirname "$0")" || exit 1
( sleep 1; open "http://127.0.0.1:$PORT/" ) &
exec python3 -I server.py --port "$PORT"
