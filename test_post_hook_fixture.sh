#!/bin/sh

printf 'hook-start pid=%s job=%s command=%s\n' "$1" "$2" "$3" \
    >> "$POST_HOOK_TEST_LOG"

if [ "${POST_HOOK_TEST_SKIP_KILL:-0}" = "1" ]; then
    printf 'hook-left-running\n' >> "$POST_HOOK_TEST_LOG"
    exit 0
fi

sleep "${POST_HOOK_TEST_DELAY:-0}"
kill -TERM -- "-$1" 2>/dev/null || kill -TERM "-$1"
printf 'hook-signal-sent\n' >> "$POST_HOOK_TEST_LOG"
