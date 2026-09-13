#!/bin/sh
# Remote-smoke start hook for the controlled container host.
#
# The smoke script runs this inside the remote project directory with
# XIUXIAN_DATA_DIR and SMOKE_OPERATION_ID exported.  The instance writes its PID
# and log next to the data directory so the stop hook can find it without
# guessing a process name.  stdin is closed so the SSH wrapper does not wait on
# an inherited descriptor.
set -eu

: "${XIUXIAN_DATA_DIR:?XIUXIAN_DATA_DIR is required}"
: "${XIUXIAN_PYTHON:=/srv/venv/bin/python}"
: "${XIUXIAN_PORT:=5898}"

rundir=$(dirname "$XIUXIAN_DATA_DIR")
mkdir -p "$XIUXIAN_DATA_DIR"

nohup env XIUXIAN_DATA_DIR="$XIUXIAN_DATA_DIR" "$XIUXIAN_PYTHON" -m nonebot_plugin_xiuxian_2 serve --port "$XIUXIAN_PORT" \
    </dev/null >"$rundir/instance.log" 2>&1 &
echo $! >"$rundir/instance.pid"
echo "started pid=$(cat "$rundir/instance.pid") port=$XIUXIAN_PORT"
