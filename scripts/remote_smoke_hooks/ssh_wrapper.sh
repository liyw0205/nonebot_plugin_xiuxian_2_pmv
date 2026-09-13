#!/bin/sh
# SSH-wrapper stand-in for the controlled container host.
#
# scripts/remote_smoke.sh transports every remote command through a single
# executable that owns authentication and runs the command string on the remote
# host.  On the Termux deployment that is
# /data/user/0/com.termux/files/home/.ssh/lmm-server.sh.  In CI there is no
# Termux host, so this wrapper provides the same contract using `docker exec`
# against an isolated container.  It prints nothing but the remote command's
# own output and never embeds credentials.
set -eu

CONTAINER=${REMOTE_CONTAINER:-xiuxian-remote}

[ $# -ge 1 ] || { echo "usage: $0 <remote command>" >&2; exit 2; }

exec docker exec "$CONTAINER" bash -lc "$1"
