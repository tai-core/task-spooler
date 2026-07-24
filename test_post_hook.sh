#!/bin/sh

set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TS="$ROOT/ts"
HOOK="$ROOT/test_post_hook_fixture.sh"
TEST_TMP=$(mktemp -d "/tmp/ts-post-hook.XXXXXX")
TS_SOCKET="$TEST_TMP/socket"
TMPDIR="$TEST_TMP"
POST_HOOK_TEST_LOG="$TEST_TMP/events.log"
export TS_SOCKET TMPDIR POST_HOOK_TEST_LOG

BG_CLIENT=
USER_CLIENT=

cleanup()
{
    "$TS" -T >/dev/null 2>&1 || true
    "$TS" -K >/dev/null 2>&1 || true
    [ -z "$BG_CLIENT" ] || wait "$BG_CLIENT" 2>/dev/null || true
    [ -z "$USER_CLIENT" ] || wait "$USER_CLIENT" 2>/dev/null || true
    rm -rf "$TEST_TMP"
}

fail()
{
    echo "FAIL: $1" >&2
    [ ! -f "$POST_HOOK_TEST_LOG" ] || {
        echo "--- hook events ---" >&2
        sed -n '1,120p' "$POST_HOOK_TEST_LOG" >&2
    }
    [ ! -f "$TS_SOCKET.error" ] || {
        echo "--- server warnings ---" >&2
        sed -n '1,160p' "$TS_SOCKET.error" >&2
    }
    exit 1
}

wait_for_state()
{
    jobid=$1
    expected=$2
    attempts=0
    while [ "$attempts" -lt 50 ]; do
        state=$("$TS" -s "$jobid" 2>/dev/null || true)
        [ "$state" != "$expected" ] || return 0
        attempts=$((attempts + 1))
        sleep 0.1
    done
    return 1
}

stop_server()
{
    "$TS" -T >/dev/null 2>&1 || true
    "$TS" -K >/dev/null 2>&1 || true
    [ -z "$BG_CLIENT" ] || wait "$BG_CLIENT" 2>/dev/null || true
    [ -z "$USER_CLIENT" ] || wait "$USER_CLIENT" 2>/dev/null || true
    BG_CLIENT=
    USER_CLIENT=
    rm -f "$TS_SOCKET" "$TS_SOCKET.error" "$POST_HOOK_TEST_LOG"
}

trap cleanup EXIT HUP INT TERM

[ -x "$TS" ] || fail "build ./ts before running this test"

echo "[1/4] --post-hook is restricted to background jobs"
if "$TS" --post-hook "$HOOK" true >/dev/null 2>&1; then
    fail "--post-hook unexpectedly accepted for a normal job"
fi
rm -f "$TS_SOCKET.error"

echo "[2/4] no hook keeps direct SIGTERM preemption"
"$TS" -S 2 >/dev/null 2>&1
"$TS" --background -f sh -c \
    'trap '\''printf "bg-exit\n" >> "$POST_HOOK_TEST_LOG"; exit 0'\'' TERM; while :; do sleep 1; done' \
    >/dev/null &
BG_CLIENT=$!
wait_for_state 0 running || fail "background job without hook did not start"

"$TS" -P 0 -f sh -c 'printf "user-start\n" >> "$POST_HOOK_TEST_LOG"' \
    >/dev/null &
USER_CLIENT=$!
wait "$USER_CLIENT"
USER_CLIENT=
wait_for_state 0 queued ||
    fail "default SIGTERM did not return the background job to queued"
grep -q '^user-start$' "$POST_HOOK_TEST_LOG" ||
    fail "normal job did not start after default preemption"
stop_server

echo "[3/4] user dispatch waits for hook-managed background exit"
POST_HOOK_TEST_DELAY=1 "$TS" -S 2 >/dev/null 2>&1
"$TS" --background --post-hook "$HOOK" -f sh -c \
    'trap '\''printf "bg-exit\n" >> "$POST_HOOK_TEST_LOG"; exit 0'\'' TERM; while :; do sleep 1; done' \
    >/dev/null &
BG_CLIENT=$!
wait_for_state 0 running || fail "background job did not start"

"$TS" -f sh -c 'printf "user-start\n" >> "$POST_HOOK_TEST_LOG"' \
    >/dev/null &
USER_CLIENT=$!
wait "$USER_CLIENT"
USER_CLIENT=

hook_line=$(grep -n '^hook-signal-sent$' "$POST_HOOK_TEST_LOG" | cut -d: -f1)
exit_line=$(grep -n '^bg-exit$' "$POST_HOOK_TEST_LOG" | cut -d: -f1)
user_line=$(grep -n '^user-start$' "$POST_HOOK_TEST_LOG" | cut -d: -f1)
[ -n "$hook_line" ] || fail "post-hook did not send the stop signal"
[ -n "$exit_line" ] || fail "background job did not report exit"
[ -n "$user_line" ] || fail "normal job did not start"
[ "$hook_line" -lt "$user_line" ] || fail "normal job started before post-hook stop"
[ "$exit_line" -lt "$user_line" ] || fail "normal job started before background exit"
stop_server

echo "[4/4] a non-stopping hook warns and keeps normal jobs queued"
POST_HOOK_TEST_SKIP_KILL=1 "$TS" -S 2 >/dev/null 2>&1
"$TS" --background --post-hook "$HOOK" -f sh -c \
    'trap '\''printf "bg-exit\n" >> "$POST_HOOK_TEST_LOG"; exit 0'\'' TERM; while :; do sleep 1; done' \
    >/dev/null &
BG_CLIENT=$!
wait_for_state 0 running || fail "background job did not start"

"$TS" -f sh -c 'printf "user-start\n" >> "$POST_HOOK_TEST_LOG"' \
    >/dev/null &
USER_CLIENT=$!
sleep 3

grep -q '^hook-left-running$' "$POST_HOOK_TEST_LOG" ||
    fail "non-stopping hook did not run"
if grep -q '^user-start$' "$POST_HOOK_TEST_LOG"; then
    fail "normal job started while hook-managed background job was alive"
fi
wait_for_state 1 queued || fail "normal job was not kept queued"
grep -q 'still running after post-hook' "$TS_SOCKET.error" ||
    fail "server did not warn about the live background job"

background_pid=$("$TS" -p 0)
kill -TERM -- "-$background_pid" 2>/dev/null ||
    kill -TERM "-$background_pid"
wait "$USER_CLIENT"
USER_CLIENT=
grep -q '^user-start$' "$POST_HOOK_TEST_LOG" ||
    fail "normal job did not start after background exit"

echo "PASS: post-hook preemption behavior"
