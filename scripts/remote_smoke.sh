#!/bin/sh
# Controlled remote backend smoke.  All destructive actions are explicit
# hooks supplied by the deployment operator; this script never guesses a
# process name or copies credentials into a command line.
set -eu

SERVER=${SERVER:-/data/user/0/com.termux/files/home/.ssh/lmm-server.sh}
REMOTE_PROJECT_DIR=${REMOTE_PROJECT_DIR:-}
REMOTE_DATA_DIR=${REMOTE_DATA_DIR:-}
REMOTE_BASE_URL=${REMOTE_BASE_URL:-http://127.0.0.1:5888}
REMOTE_PYTHON=${REMOTE_PYTHON:-python3}
LOCAL_PYTHON=${LOCAL_PYTHON:-python3}
REMOTE_STOP_COMMAND=${REMOTE_STOP_COMMAND:-}
REMOTE_START_COMMAND=${REMOTE_START_COMMAND:-}
REMOTE_STOP_NEW_COMMAND=${REMOTE_STOP_NEW_COMMAND:-}
REMOTE_WRITE_COMMAND=${REMOTE_WRITE_COMMAND:-}
REMOTE_ROLLBACK_COMMAND=${REMOTE_ROLLBACK_COMMAND:-}
REMOTE_HEALTH_TIMEOUT=${REMOTE_HEALTH_TIMEOUT:-60}
REMOTE_HEALTH_INTERVAL=${REMOTE_HEALTH_INTERVAL:-2}

die() {
    printf '%s\n' "remote smoke: $*" >&2
    exit 2
}

[ -x "$SERVER" ] || die "SERVER is not executable: $SERVER"
[ -n "$REMOTE_PROJECT_DIR" ] || die "REMOTE_PROJECT_DIR is required"
[ -n "$REMOTE_DATA_DIR" ] || die "REMOTE_DATA_DIR is required"
[ -n "$REMOTE_STOP_COMMAND" ] || die "REMOTE_STOP_COMMAND is required"
[ -n "$REMOTE_START_COMMAND" ] || die "REMOTE_START_COMMAND is required"
[ -n "$REMOTE_STOP_NEW_COMMAND" ] || die "REMOTE_STOP_NEW_COMMAND is required"
[ -n "$REMOTE_WRITE_COMMAND" ] || die "REMOTE_WRITE_COMMAND is required"
[ -n "$REMOTE_ROLLBACK_COMMAND" ] || die "REMOTE_ROLLBACK_COMMAND is required"

# Project/data paths are deliberately restricted so they can be embedded in
# the wrapper command without evaluating operator-controlled shell syntax.
case "$REMOTE_PROJECT_DIR" in
    /*) ;;
    *) die "REMOTE_PROJECT_DIR must be an absolute, shell-safe path" ;;
esac
case "$REMOTE_PROJECT_DIR" in
    *[!A-Za-z0-9_./:-]*) die "REMOTE_PROJECT_DIR must be an absolute, shell-safe path" ;;
esac
case "$REMOTE_DATA_DIR" in
    /*) ;;
    *) die "REMOTE_DATA_DIR must be an absolute, shell-safe path" ;;
esac
case "$REMOTE_DATA_DIR" in
    *[!A-Za-z0-9_./:-]*) die "REMOTE_DATA_DIR must be an absolute, shell-safe path" ;;
esac

shell_quote() {
    # POSIX single-quote escaping; values are paths/IDs, never secrets.
    printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

PROJECT_Q=$(shell_quote "$REMOTE_PROJECT_DIR")
DATA_Q=$(shell_quote "$REMOTE_DATA_DIR")
BASE_URL_Q=$(shell_quote "$REMOTE_BASE_URL")
PYTHON_Q=$(shell_quote "$REMOTE_PYTHON")
OPERATION_ID=${REMOTE_OPERATION_ID:-refactor-smoke-$(date -u +%Y%m%dT%H%M%SZ)-$$}
OPERATION_ID_Q=$(shell_quote "$OPERATION_ID")

remote_eval() {
    # The SSH wrapper owns transport/authentication.  Do not print its args.
    "$SERVER" "cd $PROJECT_Q && $1"
}

step() {
    label=$1
    command=$2
    printf '[remote-smoke] %s\n' "$label"
    remote_eval "$command"
}

printf '%s\n' "[remote-smoke] target project: $REMOTE_PROJECT_DIR"
printf '%s\n' '[remote-smoke] credentials remain inside the SSH wrapper'

printf '%s\n' '[remote-smoke] backup'
backup_json=$(remote_eval "XIUXIAN_DATA_DIR=$DATA_Q $PYTHON_Q -m nonebot_plugin_xiuxian_2 backup")
backup_path=$(printf '%s' "$backup_json" | "$LOCAL_PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["backup"])')
[ -n "$backup_path" ] || die "backup command returned no backup path"
BACKUP_Q=$(shell_quote "$backup_path")

step 'migration dry-run' "XIUXIAN_DATA_DIR=$DATA_Q $PYTHON_Q -m nonebot_plugin_xiuxian_2 migrate --dry-run"

old_stopped=0
new_started=0
rollback_done=0

cleanup() {
    status=$?
    if [ "$rollback_done" -eq 0 ] && [ "$old_stopped" -eq 1 ]; then
        if [ "$new_started" -eq 1 ]; then
            remote_eval "$REMOTE_STOP_NEW_COMMAND" >/dev/null 2>&1 || true
        fi
        # Restore the backup and bring the old instance back whenever a later
        # step fails.  The explicit hook owns the exact service manager call.
        remote_eval "export BACKUP_PATH=$BACKUP_Q XIUXIAN_DATA_DIR=$DATA_Q; $REMOTE_ROLLBACK_COMMAND" >/dev/null 2>&1 || true
    fi
    exit "$status"
}
trap cleanup EXIT HUP INT TERM

step 'stop old instance / isolate deployment' "$REMOTE_STOP_COMMAND"
old_stopped=1

step 'start new instance' "export XIUXIAN_DATA_DIR=$DATA_Q SMOKE_OPERATION_ID=$OPERATION_ID_Q; $REMOTE_START_COMMAND"
new_started=1

printf '%s\n' '[remote-smoke] readiness health check'
deadline=$(( $(date +%s) + REMOTE_HEALTH_TIMEOUT ))
while :; do
    if remote_eval "curl --fail --silent --show-error --max-time 5 $BASE_URL_Q/health/ready" >/dev/null 2>&1; then
        break
    fi
    [ "$(date +%s)" -lt "$deadline" ] || die "health check timed out"
    sleep "$REMOTE_HEALTH_INTERVAL"
done

step 'read-only manifest command' "XIUXIAN_DATA_DIR=$DATA_Q $PYTHON_Q -m nonebot_plugin_xiuxian_2 manifest >/dev/null"
step 'reversible write hook' "export XIUXIAN_DATA_DIR=$DATA_Q SMOKE_OPERATION_ID=$OPERATION_ID_Q; $REMOTE_WRITE_COMMAND"
step 'operation ledger / outbox inspection' "XIUXIAN_DATA_DIR=$DATA_Q $PYTHON_Q -m nonebot_plugin_xiuxian_2 reconcile"

step 'stop smoke instance' "$REMOTE_STOP_NEW_COMMAND"
new_started=0
step 'restore backup and old instance' "export BACKUP_PATH=$BACKUP_Q XIUXIAN_DATA_DIR=$DATA_Q; $REMOTE_ROLLBACK_COMMAND"
rollback_done=1

printf '%s\n' "[remote-smoke] PASS operation_id=$OPERATION_ID"
