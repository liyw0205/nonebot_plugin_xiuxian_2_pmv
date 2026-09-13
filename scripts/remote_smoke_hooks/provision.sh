#!/bin/sh
# Provision the controlled container host for the remote backend smoke.
#
# Creates the smoke data directory (migrated), a copy of it for the "old"
# deployment, and starts the old deployment on XIUXIAN_OLD_PORT so that the
# smoke's stop/rollback hooks have a real service to stop and restart.
#
# Run from the repository root:  sh scripts/remote_smoke_hooks/provision.sh
set -eu

CONTAINER=${XIUXIAN_CONTAINER:-xiuxian-remote}
OLD_PORT=${XIUXIAN_OLD_PORT:-5897}

docker exec "$CONTAINER" sh -c "
set -eu
pkill -f 'python -m nonebot' 2>/dev/null || true
sleep 2
rm -rf /srv/old /srv/smoke-data /srv/instance.log /srv/instance.pid
mkdir -p /srv/old
cp -a /srv/src /srv/old/src
cd /srv/src
XIUXIAN_DATA_DIR=/srv/smoke-data /srv/venv/bin/python -m nonebot_plugin_xiuxian_2 migrate >/dev/null
cp -a /srv/smoke-data /srv/old/data
cd /srv/old/src
nohup env XIUXIAN_DATA_DIR=/srv/old/data /srv/venv/bin/python -m nonebot_plugin_xiuxian_2 serve --port $OLD_PORT </dev/null >/srv/old/instance.log 2>&1 &
echo \$! >/srv/old/instance.pid
for _ in \$(seq 1 30); do
    curl -fsS http://127.0.0.1:$OLD_PORT/health/ready >/dev/null 2>&1 && break
    sleep 1
done
curl -fsS http://127.0.0.1:$OLD_PORT/health/ready >/dev/null
echo \"old deployment ready pid=\$(cat /srv/old/instance.pid) port=$OLD_PORT\"
"
