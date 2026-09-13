#!/bin/sh
# Start an isolated web instance for the browser smoke, run the smoke, stop it.
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
DATA=$(mktemp -d /tmp/xiuxian-browser-smoke-data-XXXX)
OUT=$(mktemp -d /tmp/xiuxian-browser-smoke-out-XXXX)
cd "$ROOT"

# Bind an ephemeral port instead of hard-coding one: a leftover instance from an
# interrupted run would otherwise make the smoke fail with "Address already in
# use" and mask the real result.
PORT=$("$ROOT/.venv/bin/python" - <<'PY'
import socket

with socket.socket() as probe:
    probe.bind(("127.0.0.1", 0))
    print(probe.getsockname()[1])
PY
)

XIUXIAN_DATA_DIR="$DATA" "$ROOT/.venv/bin/python" -m nonebot_plugin_xiuxian_2 migrate >"$DATA/migrate.log" 2>&1
SUPERUSERS=12345 XIUXIAN_DATA_DIR="$DATA" nohup "$ROOT/.venv/bin/python" -m nonebot_plugin_xiuxian_2 serve --port "$PORT" >"$DATA/serve.log" 2>&1 </dev/null &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT

"$ROOT/.venv/bin/python" scripts/browser_smoke.py \
  --base-url "http://127.0.0.1:$PORT" --admin-id 12345 --out "$OUT"
echo "BROWSER_SMOKE_OUT=$OUT"
