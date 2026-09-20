#!/usr/bin/env bash
#
# Run a local three-node raft-store cluster.
#
#   scripts/cluster.sh start              build and launch three nodes
#   scripts/cluster.sh status             each node's role, term, commit index
#   scripts/cluster.sh put <key> <value>  write, via any reachable node
#   scripts/cluster.sh get <key>          read, via any reachable node
#   scripts/cluster.sh kill-leader        stop whichever node currently leads
#   scripts/cluster.sh stop               shut the cluster down
#
# put and get deliberately do not target a fixed port. Which node leads, and
# which one kill-leader stops, changes from run to run, so a hardcoded address
# is a coin flip that lands on a dead node half the time. They find a
# reachable node and let the redirect carry the request to the leader.
#
# Nodes listen on 8181, 8182, and 8183. Logs are written to .cluster/.

set -euo pipefail

cd "$(dirname "$0")/.."

DIR=.cluster
PORTS=(8181 8182 8183)
IDS=(n1 n2 n3)

url_for() { echo "http://127.0.0.1:${PORTS[$1]}"; }

# peers_for builds the id=url list for every node except the given index.
peers_for() {
  local skip=$1 out=()
  for i in "${!IDS[@]}"; do
    [[ "$i" == "$skip" ]] && continue
    out+=("${IDS[$i]}=$(url_for "$i")")
  done
  local IFS=,
  echo "${out[*]}"
}

start() {
  mkdir -p "$DIR"
  go build -o bin/raftkv ./cmd/raftkv

  for i in "${!IDS[@]}"; do
    if lsof -nP -iTCP:"${PORTS[$i]}" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "port ${PORTS[$i]} is already in use; run '$0 stop' first" >&2
      exit 1
    fi
  done

  for i in "${!IDS[@]}"; do
    ./bin/raftkv \
      -id "${IDS[$i]}" \
      -addr ":${PORTS[$i]}" \
      -peers "$(peers_for "$i")" \
      > "$DIR/${IDS[$i]}.log" 2>&1 &
    echo $! > "$DIR/${IDS[$i]}.pid"
  done

  # An election needs at least one election timeout to complete.
  sleep 2
  status
  echo
  echo "logs: $DIR/*.log"
}

status() {
  for i in "${!IDS[@]}"; do
    printf '  %-3s :%s  ' "${IDS[$i]}" "${PORTS[$i]}"
    curl -sS --max-time 2 "$(url_for "$i")/status" 2>/dev/null || echo -n '{"role":"unreachable"}'
    echo
  done
}

# any_reachable prints the URL of a node that answers, or fails if none do.
any_reachable() {
  for i in "${!IDS[@]}"; do
    if curl -sS --max-time 2 -o /dev/null "$(url_for "$i")/health" 2>/dev/null; then
      url_for "$i"; return 0
    fi
  done
  echo "no node is reachable; is the cluster running?" >&2
  return 1
}

# put writes a key. -L follows the 307 redirect a follower returns, which
# preserves the method and body.
put() {
  local key=$1 value=$2 base
  base=$(any_reachable) || exit 1

  local code
  code=$(curl -sSL -o /dev/null -w '%{http_code}' -X PUT -d "$value" "$base/kv/$key")
  if [[ "$code" == "204" ]]; then
    echo "  wrote $key (via $base)"
  else
    echo "  write failed with status $code (via $base)" >&2
    exit 1
  fi
}

get() {
  local key=$1 base
  base=$(any_reachable) || exit 1

  printf '  %s -> ' "$key"
  curl -sSL -w '  [%{http_code}]\n' "$base/kv/$key"
}

leader_index() {
  for i in "${!IDS[@]}"; do
    if curl -sS --max-time 2 "$(url_for "$i")/status" 2>/dev/null | grep -q '"role":"leader"'; then
      echo "$i"; return 0
    fi
  done
  return 1
}

kill_leader() {
  local i
  if ! i=$(leader_index); then
    echo "no leader found" >&2; exit 1
  fi

  echo "stopping leader ${IDS[$i]} on :${PORTS[$i]}"
  kill -9 "$(cat "$DIR/${IDS[$i]}.pid")" 2>/dev/null || true
  rm -f "$DIR/${IDS[$i]}.pid"

  echo "waiting for the survivors to elect a new leader..."
  sleep 3
  status
}

# stop shuts the cluster down, by pid file where one exists and by listening
# port otherwise.
#
# The fallback is not belt-and-braces. Pid files are lost whenever the state
# directory is cleaned, and a stop that depends on them alone leaves orphaned
# nodes holding the ports with no supported way to release them. Falling back
# to whoever holds the port makes stop mean what it says.
stop() {
  local stopped=0

  for pidfile in "$DIR"/*.pid; do
    [[ -e "$pidfile" ]] || continue
    if kill "$(cat "$pidfile")" 2>/dev/null; then
      stopped=$((stopped + 1))
    fi
    rm -f "$pidfile"
  done

  for i in "${!IDS[@]}"; do
    local port="${PORTS[$i]}" pid
    # Only ever stop our own binary: another project's server may legitimately
    # be using one of these ports.
    for pid in $(lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null); do
      if ps -p "$pid" -o command= 2>/dev/null | grep -q 'raftkv'; then
        kill "$pid" 2>/dev/null && stopped=$((stopped + 1))
      else
        echo "  :$port is held by something that is not raftkv; leaving it alone" >&2
      fi
    done
  done

  rmdir "$DIR" 2>/dev/null || true

  if [[ "$stopped" -eq 0 ]]; then
    echo "no cluster running"
  else
    echo "cluster stopped ($stopped node(s))"
  fi
}

case "${1:-}" in
  start)       start ;;
  status)      status ;;
  put)         [[ $# -eq 3 ]] || { echo "usage: $0 put <key> <value>" >&2; exit 2; }; put "$2" "$3" ;;
  get)         [[ $# -eq 2 ]] || { echo "usage: $0 get <key>" >&2; exit 2; }; get "$2" ;;
  kill-leader) kill_leader ;;
  stop)        stop ;;
  *) echo "usage: $0 {start|status|put <k> <v>|get <k>|kill-leader|stop}" >&2; exit 2 ;;
esac
