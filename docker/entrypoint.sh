#!/bin/sh
set -eu

cd /app

if [ ! -f /app/.env ]; then
  cat >/app/.env <<'E1'
ENVIRONMENT=dev
DRIVER=~fastapi+~httpx+~websockets+~aiohttp
E1
fi

if [ ! -f /app/.env.dev ]; then
  cat >/app/.env.dev <<'E2'
LOG_LEVEL=INFO
SUPERUSERS = ["123456"]
COMMAND_START = [""]
NICKNAME = ["修仙"]
DEBUG = false
HOST = 0.0.0.0
PORT = 8080
E2
fi

mkdir -p /app/data /app/logs
export PATH="/opt/venv/bin:${PATH:-}"
export PYTHONPATH="/app/src/plugins${PYTHONPATH:+:$PYTHONPATH}"

exec "$@"
