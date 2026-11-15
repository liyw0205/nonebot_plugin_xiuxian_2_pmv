#!/bin/sh
# Start an isolated web instance for the browser smoke, run the smoke, stop it.
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
DATA=$(mktemp -d /tmp/xiuxian-browser-smoke-data-XXXX)
OUT=$(mktemp -d /tmp/xiuxian-browser-smoke-out-XXXX)
cd "$ROOT"

PYTHON_BIN="${XIUXIAN_VENV_PATH:-/home/nonebot-shared-venv}/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
    echo "browser smoke: 未找到可执行的共享虚拟环境 Python: $PYTHON_BIN" >&2
    echo "请先准备 /home/nonebot-shared-venv，或设置 XIUXIAN_VENV_PATH=PATH 后重试。" >&2
    exit 2
fi

# Bind an ephemeral port instead of hard-coding one: a leftover instance from an
# interrupted run would otherwise make the smoke fail with "Address already in
# use" and mask the real result.
PORT=$("$PYTHON_BIN" - <<'PY'
import socket

with socket.socket() as probe:
    probe.bind(("127.0.0.1", 0))
    print(probe.getsockname()[1])
PY
)

XIUXIAN_DATA_DIR="$DATA" "$PYTHON_BIN" -m nonebot_plugin_xiuxian_2 migrate >"$DATA/migrate.log" 2>&1
SUPERUSERS=12345 XIUXIAN_DATA_DIR="$DATA" nohup "$PYTHON_BIN" -m nonebot_plugin_xiuxian_2 serve --port "$PORT" >"$DATA/serve.log" 2>&1 </dev/null &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT

"$PYTHON_BIN" scripts/browser_smoke.py \
  --base-url "http://127.0.0.1:$PORT" --admin-id 12345 --out "$OUT"
echo "BROWSER_SMOKE_OUT=$OUT"
