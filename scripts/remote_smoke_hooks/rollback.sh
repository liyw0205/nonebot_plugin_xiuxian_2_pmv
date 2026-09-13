#!/bin/sh
# Remote-smoke rollback hook for the controlled container host.
#
# Restores the backup produced by the smoke run (BACKUP_PATH), verifies the
# restored marker is gone, and brings the pre-existing "old" instance back up.
# The old instance here is the previously deployed copy of the project under
# /srv/old, so the hook also proves the rollback path really restarts a service
# rather than only deleting files.
set -eu

: "${XIUXIAN_DATA_DIR:?XIUXIAN_DATA_DIR is required}"
: "${BACKUP_PATH:?BACKUP_PATH is required}"
: "${XIUXIAN_PYTHON:=/srv/venv/bin/python}"
: "${XIUXIAN_OLD_DIR:=/srv/old}"
: "${XIUXIAN_OLD_PORT:=5897}"

cd "${XIUXIAN_PROJECT_DIR:-/srv/src}"
# restore() re-hashes every manifest entry before copying, so a checksum
# mismatch aborts here instead of silently installing a corrupt database.
"$XIUXIAN_PYTHON" -m nonebot_plugin_xiuxian_2 restore --backup "$BACKUP_PATH"

# Undo the smoke run's own reversible write.  The marker lives next to the
# databases rather than inside them, so the database restore above cannot
# remove it; the hook owns reverting the artifact it created.
marker="$XIUXIAN_DATA_DIR/remote-smoke-marker.json"
if [ -f "$marker" ]; then
    rm -f "$marker"
fi
if [ -f "$marker" ]; then
    echo "rollback failed: smoke marker still present" >&2
    exit 1
fi
echo "rollback verified: backup restored and smoke marker removed"

# Restart the previous deployment that the smoke run isolated.  It reuses the
# same rundir convention as start.sh (instance.pid next to the data directory's
# parent) so REMOTE_STOP_COMMAND can stop it with the ordinary stop hook.
if [ -d "$XIUXIAN_OLD_DIR" ]; then
    old_pidfile="$XIUXIAN_OLD_DIR/instance.pid"
    if [ -f "$old_pidfile" ]; then
        old_pid=$(cat "$old_pidfile" 2>/dev/null || true)
        if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
            kill "$old_pid" 2>/dev/null || true
            for _ in 1 2 3 4 5; do
                kill -0 "$old_pid" 2>/dev/null || break
                sleep 1
            done
            kill -9 "$old_pid" 2>/dev/null || true
        fi
        rm -f "$old_pidfile"
    fi
    mkdir -p "$XIUXIAN_OLD_DIR/data"
    nohup env XIUXIAN_DATA_DIR="$XIUXIAN_OLD_DIR/data" "$XIUXIAN_PYTHON" -m nonebot_plugin_xiuxian_2 serve --port "$XIUXIAN_OLD_PORT" \
        </dev/null >"$XIUXIAN_OLD_DIR/instance.log" 2>&1 &
    echo $! >"$XIUXIAN_OLD_DIR/instance.pid"
    echo "old instance restarted pid=$(cat "$XIUXIAN_OLD_DIR/instance.pid") port=$XIUXIAN_OLD_PORT"
fi
