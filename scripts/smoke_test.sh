#!/usr/bin/env bash
# Smoke-test built irtt-rs binaries end to end on the local host: start the
# server on loopback, run a short client test against it, and check that the
# dispatcher and every applet start.
#
# Usage: scripts/smoke_test.sh [BIN_DIR]   (default: target/debug)
#
# Runs under bash on Linux, macOS, and Windows (Git Bash); Windows resolves
# the .exe suffix itself.

set -euo pipefail

bin_dir="${1:-target/debug}"
addr="127.0.0.1:${IRTT_SMOKE_PORT:-21120}"
work_dir="$(mktemp -d)"
server_log="$work_dir/server.log"
client_log="$work_dir/client.log"
server_pid=""

cleanup() {
    if [[ -n "$server_pid" ]]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    rm -rf "$work_dir"
}
trap cleanup EXIT

fail() {
    echo "smoke test failed: $*" >&2
    for log in "$server_log" "$client_log"; do
        if [[ -s "$log" ]]; then
            echo "--- $(basename "$log")" >&2
            cat "$log" >&2
        fi
    done
    exit 1
}

echo "== versions"
"$bin_dir/irtt-rs" --version
for applet in irtt-client irtt-server irtt-tui; do
    "$bin_dir/$applet" --version || fail "$applet --version exited non-zero"
done

echo "== server on $addr"
"$bin_dir/irtt-server" --bind "$addr" >"$server_log" 2>&1 &
server_pid=$!
for _ in $(seq 1 100); do
    grep -q "listening on" "$server_log" 2>/dev/null && break
    kill -0 "$server_pid" 2>/dev/null || fail "server exited during startup"
    sleep 0.1
done
grep -q "listening on" "$server_log" || fail "server did not start listening"

echo "== client against $addr"
"$bin_dir/irtt-client" -d 2s -i 100ms "$addr" >"$client_log" 2>&1 \
    || fail "client exited non-zero"
summary="$(grep '^packets:' "$client_log")" || fail "client printed no packet summary"
echo "$summary"
[[ "$summary" =~ received=([0-9]+) ]] || fail "unparseable summary: $summary"
((BASH_REMATCH[1] > 0)) || fail "no echo replies received"

echo "== dispatcher client applet"
"$bin_dir/irtt-rs" client -d 500ms -i 100ms "$addr" >"$client_log" 2>&1 \
    || fail "irtt-rs client exited non-zero"

kill -0 "$server_pid" 2>/dev/null || fail "server exited during the test"
echo "smoke test passed"
