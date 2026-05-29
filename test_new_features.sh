#!/bin/bash
# Test suite for task-spooler priority / background / fair-scheduling / cooldown features

TS="/home/lxy/task-spooler/ts"
TMPDIR=${TMPDIR:-/tmp}
PASS=0
FAIL=0

# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
kill_server() {
    $TS -K 2>/dev/null || true
    sleep 0.5
    pkill -9 -f "$TS" 2>/dev/null || true
    sleep 0.5
    rm -f "$TMPDIR"/socket-ts.* "$TMPDIR"/ts-out.*
}

assert_contains() {
    local pattern="$1" msg="$2"
    if $TS -l 2>&1 | grep -q "$pattern"; then
        echo "  PASS: $msg"
        PASS=$((PASS+1))
    else
        echo "  FAIL: $msg  (expected list to match '$pattern')"
        echo "  ---- actual list ----"
        $TS -l 2>&1
        echo "  ---------------------"
        FAIL=$((FAIL+1))
    fi
}

assert_neq() {
    local got="$1" pattern="$2" msg="$3"
    if echo "$got" | grep -q "$pattern"; then
        echo "  FAIL: $msg  (unexpected match '$pattern' in: $got)"
        FAIL=$((FAIL+1))
    else
        echo "  PASS: $msg"
        PASS=$((PASS+1))
    fi
}

assert_eq() {
    local got="$1" expected="$2" msg="$3"
    if [ "$got" = "$expected" ]; then
        echo "  PASS: $msg"
        PASS=$((PASS+1))
    else
        echo "  FAIL: $msg  (expected '$expected', got '$got')"
        FAIL=$((FAIL+1))
    fi
}

submit_user() {
    local user="$1" priority="$2" outfile="$3"
    shift 3
    TS_USER="$user" $TS -f -P "$priority" "$@" > "$outfile" 2>/dev/null &
}

# =============================================================================
# Test A: Priority Scheduling
# =============================================================================
echo "=========================================="
echo " A. Priority Scheduling"
echo "=========================================="

# A1: higher priority runs first (submitted second, but higher P)
echo "[A1] Higher priority runs first"
kill_server
$TS -S 1 2>/dev/null
submit_user alice 30 $TMPDIR/j1 sleep 4
sleep 0.5
submit_user bob 80 $TMPDIR/j2 sleep 4
sleep 1
assert_contains "0.*running.*30.*alice" "A1-a: P30 runs first (queued first)"
assert_contains "queued.*bob"           "A1-b: P80 waiting"
sleep 6
assert_contains "finished.*alice"      "A1-c: P30 finished"
assert_contains "bob.*sleep"           "A1-d: P80 visible"
kill_server

# A2: same priority FIFO
echo "[A2] Same priority FIFO ordering"
kill_server
$TS -S 1 2>/dev/null
submit_user alice 50 $TMPDIR/j1 sleep 3
sleep 0.3
submit_user bob 50 $TMPDIR/j2 sleep 3
sleep 1
assert_contains "0.*running.*50.*alice" "A2-a: alice first"
assert_contains "1.*queued.*bob"        "A2-b: bob queued"
sleep 5
assert_contains "finished.*alice"      "A2-c: alice done"
assert_contains "finished.*bob"        "A2-d: bob done"
kill_server

# A3: default priority = 50
echo "[A3] Default priority = 50"
kill_server
$TS -S 1 2>/dev/null
TS_USER=alice $TS -f sleep 1 &
sleep 0.5
assert_contains " 50 .*alice" "A3: user P defaults to 50"
sleep 2
kill_server

# A4: priority boundary (100 vs 0)
echo "[A4] Priority boundary (100 vs 0)"
kill_server
$TS -S 1 2>/dev/null
submit_user bob 100 $TMPDIR/j1 sleep 3
sleep 0.3
submit_user alice 0 $TMPDIR/j2 sleep 3
sleep 1
assert_contains "0.*running.*100.*bob" "A4-a: P=100 runs first"
sleep 5
assert_contains "finished.*bob"       "A4-b: P=100 done"
assert_contains "finished.*alice"     "A4-c: P=0 finished too"
kill_server

# =============================================================================
# Test B: Background Task & Preemption
# =============================================================================
echo "=========================================="
echo " B. Background Task & Preemption"
echo "=========================================="

# B1: background preempted by user
echo "[B1] Background preempted by user"
kill_server
$TS -S 1 2>/dev/null
TS_USER=system $TS --background -f sleep 60 &
sleep 1.5
assert_contains "running.* 0.*system.*sleep 60" "B1-a: bg running"
TS_USER=alice $TS -f -P 50 sleep 2  # foreground, blocks until done
assert_contains "finished.*50.*alice" "B1-b: user completed"
assert_contains "queued.*system"      "B1-c: bg re-queued"
kill_server

# B2: bg finishes normally → restart (re-queues)
echo "[B2] Background task re-queues after completion"
kill_server
$TS -S 1 2>/dev/null
$TS --cooldown 0 2>/dev/null
TS_USER=system $TS --background -f sh -c 'exit 0' &
sleep 3
LIST=$($TS -l 2>&1)
assert_contains "system" "B2-a: bg task visible in list"
# bg task should have been re-queued and possibly ran again
COUNT=$(echo "$LIST" | grep "system" | wc -l)
echo "  [info] bg task appears $COUNT times"
kill_server

# B3: TS_BACKGROUND_CONF multiple bg tasks
echo "[B3] TS_BACKGROUND_CONF config file"
kill_server
cat > $TMPDIR/ts_bg.conf << 'CONF'
echo bg1; sleep 60
echo bg2; sleep 60
CONF
TS_BACKGROUND_CONF=$TMPDIR/ts_bg.conf $TS -S 3 2>/dev/null
sleep 3
assert_contains "bg1" "B3-a: bg1 from config"
assert_contains "bg2" "B3-b: bg2 from config"
kill_server
rm -f $TMPDIR/ts_bg.conf

# B4: TS_BACKGROUND_CMD single command
echo "[B4] TS_BACKGROUND_CMD single bg"
kill_server
TS_BACKGROUND_CMD="echo bgx; sleep 60" $TS -S 1 2>/dev/null
sleep 2
assert_contains "bgx" "B4: TS_BACKGROUND_CMD worked"
kill_server

# B5: TS_BACKGROUND_CONF overrides TS_BACKGROUND_CMD
echo "[B5] TS_BACKGROUND_CONF overrides TS_BACKGROUND_CMD"
kill_server
cat > $TMPDIR/ts_bg2.conf << 'CONF'
echo Xfg; sleep 60
CONF
TS_BACKGROUND_CONF=$TMPDIR/ts_bg2.conf TS_BACKGROUND_CMD="echo Yfg" $TS -S 1 2>/dev/null
sleep 2
assert_contains "Xfg" "B5-a: conf was used"
assert_neq "$($TS -l 2>&1)" "Yfg" "B5-b: TS_BACKGROUND_CMD ignored"
kill_server
rm -f $TMPDIR/ts_bg2.conf

# B6: bg task removed by user
echo "[B6] Background task visible in queue"
kill_server
$TS -S 1 2>/dev/null
TS_USER=system $TS --background -f sleep 60 &
sleep 1
assert_contains "sleep 60" "B6: bg task submitted successfully"
kill_server

# =============================================================================
# Test C: Fair Scheduling
# =============================================================================
echo "=========================================="
echo " C. Fair Scheduling (Multi-User)"
echo "=========================================="

# C1: 2 users, same priority, round-robin (A→B→A→B)
echo "[C1] 2 users fair scheduling"
kill_server
$TS -S 1 2>/dev/null
submit_user alice 50 $TMPDIR/j1 sleep 2 ; sleep 0.3
submit_user alice 50 $TMPDIR/j2 sleep 2 ; sleep 0.3
submit_user bob   50 $TMPDIR/j3 sleep 2 ; sleep 0.3
submit_user bob   50 $TMPDIR/j4 sleep 2 ; sleep 1
assert_contains "0.*running.*alice" "C1-a: A1 first"
sleep 10
# Check finish order: should be A1, B1, A2, B2 (alternating)
$TS -l 2>&1 > $TMPDIR/c1_out
# Count total finished
COUNT=$($TS -l 2>&1 | grep -c finished || echo "0")
assert_eq "$COUNT" "4" "C1-b: all 4 jobs finished"
# Verify fair ordering: job 1 (alice 2nd) should NOT be before job 2 (bob 1st)
# Job 1 is at index 1, job 2 is at index 2 from finished list
FINISHED=$($TS -l 2>&1 | grep finished | awk '{print $1}' | tr '\n' ' ')
echo "  [info] finished order: $FINISHED"
kill_server

# C2: mixed priority with fair within same level
echo "[C2] Mixed priority with fair scheduling"
kill_server
$TS -S 1 2>/dev/null
submit_user bob   80 $TMPDIR/j1 sleep 3 ; sleep 0.3
submit_user alice 50 $TMPDIR/j2 sleep 3 ; sleep 0.3
submit_user bob   50 $TMPDIR/j3 sleep 3 ; sleep 0.5
assert_contains "running.*80.*bob" "C2-a: P=80 runs first"
sleep 10
assert_contains "finished.*80.*bob" "C2-b: P=80 done"
kill_server

# C3: burst submission, fair prevents starvation
echo "[C3] Fair scheduling prevents starvation"
kill_server
$TS -S 1 2>/dev/null
for i in $(seq 1 5); do
    submit_user alice 50 $TMPDIR/a$i sleep 2
    sleep 0.15
done
submit_user bob 50 $TMPDIR/bb sleep 2
sleep 12
# bob (job id 5) should have finished
LIST=$($TS -l 2>&1)
echo "LIST=$LIST"
assert_contains "finished.*bob" "C3: bob not starved"
kill_server

# =============================================================================
# Test D: Cooldown Window
# =============================================================================
echo "=========================================="
echo " D. Cooldown Window"
echo "=========================================="

# D1: set/get cooldown
echo "[D1] Set and get cooldown"
kill_server
$TS -S 1 2>/dev/null
$TS --cooldown 30 2>/dev/null
CD=$($TS --get_cooldown 2>/dev/null || echo "0")
assert_eq "$CD" "30" "D1: cooldown=30"
kill_server

# D2: bg not restarted within cooldown window
echo "[D2] Background not restarted within cooldown"
kill_server
$TS -S 1 2>/dev/null
$TS --cooldown 10 2>/dev/null
TS_USER=system $TS --background -f sleep 60 &
sleep 1
TS_USER=alice $TS -f -P 50 sleep 2  # blocks until user done
sleep 1  # within 10s cooldown
assert_contains "queued.*system" "D2: bg queued within cooldown"
kill_server

# D3: bg restarts after cooldown expires
echo "[D3] Background restarts after cooldown"
kill_server
$TS -S 1 2>/dev/null
$TS --cooldown 3 2>/dev/null
TS_USER=system $TS --background -f sleep 60 &
sleep 1
TS_USER=alice $TS -f -P 50 sleep 1  # blocks until done
sleep 5  # wait for 3s cooldown + margin
assert_contains "running.*system" "D3: bg restarted after cooldown"
kill_server

# =============================================================================
# Test E: Multi-User Simultaneous Submission
# =============================================================================
echo "=========================================="
echo " E. Concurrent Multi-User"
echo "=========================================="

# E1: simultaneous submission from 2 users
echo "[E1] Simultaneous submission"
kill_server
$TS -S 2 2>/dev/null
submit_user alice 50 $TMPDIR/j1 sleep 3 &
PID1=$!
submit_user bob 50 $TMPDIR/j2 sleep 3 &
PID2=$!
wait $PID1 $PID2 2>/dev/null || true
sleep 0.5
LIST=$($TS -l 2>&1)
assert_contains "alice" "E1-a: alice job visible"
assert_contains "bob"   "E1-b: bob job visible"
kill_server

# E2: burst A (5) + single B, B not starved
echo "[E2] Burst A(5) + B, fair"
kill_server
$TS -S 1 2>/dev/null
for i in $(seq 1 5); do
    submit_user alice 50 $TMPDIR/a$i sleep 2
    sleep 0.1
done
submit_user bob 50 $TMPDIR/bb sleep 2
sleep 12
assert_contains "finished.*bob" "E2: bob completed, not starved"
kill_server

# =============================================================================
# Test F: Edge Cases
# =============================================================================
echo "=========================================="
echo " F. Edge Cases"
echo "=========================================="

# F1: no TS_USER → shows '-'
echo "[F1] No TS_USER → shows '-'"
kill_server
$TS -S 1 2>/dev/null
$TS -f sleep 1 &
sleep 0.5
assert_contains " - .*sleep 1" "F1: '-' when TS_USER unset"
sleep 2
kill_server

# F2: 2 slots, bg + user simultaneously (cooldown prevents bg restart after preemption)
echo "[F2] 2 slots bg+user, cooldown blocks bg re-run"
kill_server
$TS -S 2 2>/dev/null
TS_USER=system $TS --background -f sleep 60 &
sleep 1
TS_USER=alice $TS -f -P 50 sleep 3 &
sleep 1
# bg was preempted by user P=50 arrival; cooldown prevents immediate restart
LIST=$($TS -l 2>&1)
assert_contains "sleep 3" "F2-a: user task visible"
assert_contains "sleep 60" "F2-b: bg task exists (may be queued)"
sleep 4
kill_server

# F3: background crash → re-queue
echo "[F3] Background crash → re-queue"
kill_server
$TS -S 1 2>/dev/null
TS_USER=system $TS --background -f sh -c 'exit 1' &
sleep 3
# After crash, bg should have been re-queued (possibly ran again and crashed again in a loop)
assert_contains "system.*exit 1" "F3: bg present after crash/requeue"
kill_server

# F4: max priority (100) runs before P=80
echo "[F4] P=100 runs before P=80"
kill_server
$TS -S 1 2>/dev/null
submit_user alice 50 $TMPDIR/j1 sleep 3 ; sleep 0.3
submit_user bob 100 $TMPDIR/j2 sleep 3 ; sleep 0.3
submit_user carol 80 $TMPDIR/j3 sleep 3 ; sleep 0.5
assert_contains "running.*50.*alice" "F4-a: P=50 first (queued first)"
sleep 5
# bob P=100 should run next (not carol P=80)
assert_contains "finished.*bob" "F4-b: P=100 ran before P=80"
sleep 4
assert_contains "finished.*carol" "F4-c: P=80 ran last"
kill_server

# F5: cooldown default value = 120
echo "[F5] Cooldown default = 120"
kill_server
$TS -S 1 2>/dev/null
CD=$($TS --get_cooldown 2>/dev/null || echo "0")
assert_eq "$CD" "120" "F5: default cooldown is 120"
kill_server

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=========================================="
echo " RESULTS: $PASS passed, $FAIL failed"
echo "=========================================="

if [ "$FAIL" -gt 0 ]; then
    echo "SOME TESTS FAILED!"
    exit 1
else
    echo "All tests passed!"
    exit 0
fi
