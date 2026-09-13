#!/bin/sh
# Convenience runner for the controlled container-host remote smoke.
#
# Wires scripts/remote_smoke.sh to the docker-based SSH-wrapper stand-in and the
# hook scripts in this directory.  Every hook path is the container-side copy
# under /srv/src, because remote_smoke.sh executes the hook strings on the
# remote host.  Override any REMOTE_* variable in the environment beforehand.
# This script only orchestrates; the evidence comes from scripts/remote_smoke.sh.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/../.." && pwd)

REMOTE_PROJECT_DIR=${REMOTE_PROJECT_DIR:-/srv/src}
REMOTE_DATA_DIR=${REMOTE_DATA_DIR:-/srv/smoke-data}
REMOTE_OLD_DIR=${REMOTE_OLD_DIR:-/srv/old}
REMOTE_HOOKS=$REMOTE_PROJECT_DIR/scripts/remote_smoke_hooks

export SERVER=${SERVER:-$HERE/ssh_wrapper.sh}
export REMOTE_PROJECT_DIR
export REMOTE_DATA_DIR
export REMOTE_BASE_URL=${REMOTE_BASE_URL:-http://127.0.0.1:5898}
export REMOTE_PYTHON=${REMOTE_PYTHON:-/srv/venv/bin/python}
# The pre-existing deployment lives in its own directory, so the stop hook
# resolves its pid file from that data directory rather than the smoke one.
export REMOTE_STOP_COMMAND=${REMOTE_STOP_COMMAND:-"XIUXIAN_DATA_DIR=$REMOTE_OLD_DIR/data $REMOTE_HOOKS/stop.sh"}
export REMOTE_START_COMMAND=${REMOTE_START_COMMAND:-"XIUXIAN_PROJECT_DIR=$REMOTE_PROJECT_DIR $REMOTE_HOOKS/start.sh"}
export REMOTE_STOP_NEW_COMMAND=${REMOTE_STOP_NEW_COMMAND:-"XIUXIAN_DATA_DIR=$REMOTE_DATA_DIR $REMOTE_HOOKS/stop.sh"}
export REMOTE_WRITE_COMMAND=${REMOTE_WRITE_COMMAND:-"$REMOTE_PYTHON scripts/remote_smoke_write.py"}
export REMOTE_ROLLBACK_COMMAND=${REMOTE_ROLLBACK_COMMAND:-"XIUXIAN_PROJECT_DIR=$REMOTE_PROJECT_DIR XIUXIAN_OLD_DIR=$REMOTE_OLD_DIR $REMOTE_HOOKS/rollback.sh"}

exec "$PROJECT_ROOT/scripts/remote_smoke.sh"
