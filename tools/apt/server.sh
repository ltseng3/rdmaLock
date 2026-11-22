#!/usr/bin/env bash
# Run directly on a CloudLab replica. Never leaves a stale server PID untracked.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KIND="${1:-}"; ACTION="${2:-}"
DEV="${IB_DEV:-mlx4_0}"
IB_PORT="${IB_PORT:-1}"
CONTROL_PORT="${CONTROL_PORT:-29660}"
RELEASE_PORT="${RELEASE_PORT:-29670}"
mkdir -p "$ROOT/logs"
if [[ "$KIND" != ordering && "$KIND" != release ]]; then
  echo "Usage: $0 <ordering|release> <start|stop|restart|status>" >&2; exit 2
fi
PIDFILE="$ROOT/logs/${KIND}.pid"
LOG="$ROOT/logs/${KIND}_server.log"
stop() {
  if [[ -f "$PIDFILE" ]]; then
    local pid
    pid="$(cat "$PIDFILE")"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true
      for _ in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.1
      done
      if kill -0 "$pid" 2>/dev/null; then echo "Server ${pid} still running" >&2; return 1; fi
    fi
    rm -f "$PIDFILE"
  fi
}
start() {
  if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    echo "Existing ${KIND} server running; use restart" >&2; return 1
  fi
  if [[ "$KIND" == ordering ]]; then
    local binary="$ROOT/mucql/build-apt/mucql_rdma_paxos"
    nohup env MUCQL_IB_PORT="$IB_PORT" "$binary" server "$CONTROL_PORT" "$DEV" > "$LOG" 2>&1 < /dev/null &
    echo $! > "$PIDFILE"
  else
    local binary="$ROOT/mucql/build-apt/mucql_verbs_release"
    nohup env MUCQL_IB_PORT="$IB_PORT" "$binary" server "$RELEASE_PORT" "$DEV" 200000 > "$LOG" 2>&1 < /dev/null &
    echo $! > "$PIDFILE"
  fi
  sleep 1
  if ! kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    tail -n 30 "$LOG" >&2 || true; echo "${KIND} server exited during startup" >&2; return 1
  fi
  echo "${KIND} server PID $(cat "$PIDFILE") (log: $LOG)"
}
case "$ACTION" in
  start) start;;
  stop) stop;;
  restart) stop; start;;
  status)
    if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      echo "${KIND} server active PID=$(cat "$PIDFILE")"
    else
      echo "${KIND} server not running"; exit 1
    fi;;
  *) echo "Usage: $0 <ordering|release> <start|stop|restart|status>" >&2; exit 2;;
esac
