#!/bin/sh
# PID 1 helper: on SIGTERM ask the daemon to write the encrypted vault before exiting.
STOP="${CTDOZ_STOP_FILE:-/tmp/ctdoz.stop}"
rm -f "$STOP"
ctdoz-daemon "$@" &
pid=$!
trap 'touch "$STOP"; wait $pid; exit $?' TERM INT
wait $pid
