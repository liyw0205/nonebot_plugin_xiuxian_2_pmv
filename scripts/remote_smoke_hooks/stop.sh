#!/bin/sh
# Remote-smoke stop hook for the controlled container host.
#
# Stops the instance started by start.sh using the PID file it wrote.  Safe to
# run when nothing is running, which keeps the smoke script's cleanup trap
# idempotent.
set -eu

: "${XIUXIAN_DATA_DIR:?XIUXIAN_DATA_DIR is required}"

rundir=$(dirname "$XIUXIAN_DATA_DIR")
pidfile="$rundir/instance.pid"

if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 1
        done
        kill -9 "$pid" 2>/dev/null || true
        echo "stopped pid=$pid"
    else
        echo "no running pid $pid"
    fi
    rm -f "$pidfile"
else
    echo "no pid file"
fi
