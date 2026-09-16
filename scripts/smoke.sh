#!/usr/bin/env bash
#
# End-to-end smoke test against a running raft-store node.
#
# Unlike the Go unit tests, this exercises the real binary over a real socket,
# so it catches wiring mistakes that in-process tests cannot: routing, status
# codes, and serialization as an actual client observes them.
#
# Usage:  scripts/smoke.sh [base-url]        (default: http://127.0.0.1:8081)

set -euo pipefail

BASE="${1:-http://127.0.0.1:8081}"
KEY="smoke-$$"          # unique per run, so a failed run cannot poison the next
PASS=0
FAIL=0

green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }

# check <description> <expected-status> <curl-args...>
check() {
  local desc="$1" want="$2"; shift 2
  local body status

  # Write the body to stdout and the status code to the last line, so a single
  # curl invocation yields both without a temp file.
  body=$(curl -sS -w $'\n%{http_code}' "$@" 2>/dev/null) || {
    printf '  %s  %s (could not reach %s)\n' "$(red FAIL)" "$desc" "$BASE"
    FAIL=$((FAIL + 1)); return 0
  }

  status="${body##*$'\n'}"
  body="${body%$'\n'*}"
  body="${body%$'\n'}"   # JSON responses end in a newline; drop it for tidy output

  if [[ "$status" == "$want" ]]; then
    printf '  %s  %-38s %s %s\n' "$(green PASS)" "$desc" "$status" "$body"
    PASS=$((PASS + 1))
  else
    printf '  %s  %-38s got %s, want %s %s\n' "$(red FAIL)" "$desc" "$status" "$want" "$body"
    FAIL=$((FAIL + 1))
  fi
}

printf '\nsmoke test against %s\n\n' "$BASE"

check "health"                    200 "$BASE/health"
check "get absent key"            404 "$BASE/kv/$KEY"
check "put new key"               204 -X PUT -d 'hello raft' "$BASE/kv/$KEY"
check "get stored value"          200 "$BASE/kv/$KEY"
check "overwrite existing key"    204 -X PUT -d 'second value' "$BASE/kv/$KEY"
check "get overwritten value"     200 "$BASE/kv/$KEY"
check "put empty value"           204 -X PUT -d '' "$BASE/kv/$KEY-empty"
check "get empty value"           200 "$BASE/kv/$KEY-empty"
check "delete existing key"       204 -X DELETE "$BASE/kv/$KEY"
check "get after delete"          404 "$BASE/kv/$KEY"
check "delete absent key"         404 -X DELETE "$BASE/kv/$KEY"
check "unsupported method"        405 -X POST "$BASE/kv/$KEY"
check "unknown path"              404 "$BASE/nope"
check "missing key segment"       404 "$BASE/kv/"
check "key containing a slash"    404 "$BASE/kv/a/b"

# Leave no residue behind, so repeated runs start from the same state.
curl -sS -o /dev/null -X DELETE "$BASE/kv/$KEY-empty" 2>/dev/null || true

printf '\n  %d passed, %d failed\n\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]
