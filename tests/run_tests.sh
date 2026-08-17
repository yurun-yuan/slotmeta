#!/bin/bash
# slotmeta end-to-end tests. Run anywhere -- no scheduler, no GPU:
#
#     tests/run_tests.sh
#
# The whole protocol is filesystem-based, so pointing SLOT_ROOT at a scratch dir and
# stubbing `nvidia-smi` exercises the real supervisor and the real slotctl. What
# cannot be faked here is a genuine Ray teardown; the sweep is instead driven with a
# process that escapes its group exactly the way raylet does.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/slotmeta-test.XXXXXX")"
export SLOT_ROOT="$TMP/slots"
export SLOT_REPO="$TMP"
export SLOT_SCHEDULER="${SLOT_SCHEDULER:-condor}"
export _CONDOR_SCRATCH_DIR="$TMP/scratch"; mkdir -p "$_CONDOR_SCRATCH_DIR"
export SLOT_POLL=2 SLOT_MAX_IDLE=600 SLOT_MIN_TASK_TIME=5 SLOT_KILL_GRACE=6
export PATH="$TMP/stub:$PATH"
mkdir -p "$TMP/stub" "$TMP/scripts"

PASS=0; FAIL=0
ok()   { echo "  PASS  $*"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL  $*"; FAIL=$((FAIL+1)); }
check(){ if eval "$2"; then ok "$1"; else bad "$1"; fi; }

# nvidia-smi stub: reports whatever pids are listed in $TMP/gpu_holders
cat > "$TMP/stub/nvidia-smi" <<EOF
#!/bin/bash
case "\$*" in
  *compute-apps*) [ -f "$TMP/gpu_holders" ] && while read -r p; do
                    [ -n "\$p" ] && kill -0 "\$p" 2>/dev/null && echo "\$p"; done < "$TMP/gpu_holders" ;;
  *) exit 0 ;;
esac
exit 0
EOF
chmod +x "$TMP/stub/nvidia-smi"; : > "$TMP/gpu_holders"

cat > "$TMP/scripts/long.sh" <<'EOF'
#!/bin/bash
echo "VERSION=ORIGINAL"
trap 'echo "got SIGTERM"; exit 143' TERM
for i in $(seq 1 900); do echo "tick $i"; sleep 1; done
EOF
# spawns a child that setsid()s away and is then reparented -- how ray daemons survive
cat > "$TMP/scripts/escapes.sh" <<EOF
#!/bin/bash
setsid bash -c 'echo \$\$ >> "$TMP/gpu_holders"; exec sleep 900' &
sleep 1
echo "spawned escapee"
for i in \$(seq 1 900); do echo "tick \$i"; sleep 1; done
EOF
chmod +x "$TMP/scripts"/*.sh

sup() { SLOT_MAX_LIFETIME="$2" nohup bash "$ROOT/slot_supervisor.sh" "$1" "$2" \
          > "$TMP/$1.out" 2>&1 & echo $!; }
ctl() { python3 "$ROOT/slotctl" "$@"; }
state() { python3 -c "
import json,sys
try: print(json.load(open('$SLOT_ROOT/$1/state.json')).get('$2',''))
except Exception: print('')"; }

echo "== 1. supervisor comes up, reports state =="
P=$(sup t1 900); sleep 5
check "state.json is live"          '[ -n "$(state t1 state)" ]'
check "scheduler recorded"          '[ "$(state t1 scheduler)" = "condor" ]'
check "slotctl list sees it"        'ctl list 2>/dev/null | grep -q t1'

echo "== 2. run a task =="
ctl run t1 scripts/long.sh --label alpha --port 7101 >/dev/null 2>&1; sleep 5
check "task running"                '[ "$(state t1 task_running)" = "1" ]'
check "label recorded"              '[ "$(state t1 task_label)" = "alpha" ]'
check "task log has output"         'grep -q ORIGINAL "$SLOT_ROOT/t1/tasks/000001/task.log"'

echo "== 3. snapshot isolates a mid-run edit =="
check "snapshot exists"             '[ -f "$SLOT_ROOT/t1/tasks/000001/snap/long.sh" ]'
echo '#!/bin/bash
echo VERSION=EDITED' > "$TMP/scripts/long.sh"
check "snapshot kept ORIGINAL"      'grep -q "VERSION=ORIGINAL" "$SLOT_ROOT/t1/tasks/000001/snap/long.sh"'
check "live file is EDITED"         'grep -q "VERSION=EDITED" "$TMP/scripts/long.sh"'

echo "== 4. hot swap =="
ctl run t1 scripts/escapes.sh --label beta --port 7102 >/dev/null 2>&1; sleep 8
check "new task is running"         '[ "$(state t1 task_label)" = "beta" ]'
check "old task recorded killed"    'grep -q killed "$SLOT_ROOT/t1/tasks/000001/exit"'
check "old task saw SIGTERM"        'grep -q "got SIGTERM" "$SLOT_ROOT/t1/tasks/000001/task.log"'

echo "== 5. teardown catches a process that escaped the group =="
ESC=$(tail -1 "$TMP/gpu_holders" 2>/dev/null)
check "escapee exists"              '[ -n "$ESC" ] && kill -0 "$ESC" 2>/dev/null'
check "escapee left the group"      '[ "$(ps -o pgid= -p $ESC | tr -d " ")" != "$(cat $SLOT_ROOT/t1/tasks/000002/pid)" ]'
ctl idle t1 >/dev/null 2>&1; sleep 12
check "escapee was killed"          '! kill -0 "$ESC" 2>/dev/null'
check "gpu reported clear"          '[ "$(state t1 gpu_dirty)" = "0" ]'

echo "== 7. stop releases the slot =="
ctl stop t1 >/dev/null 2>&1; sleep 5
check "supervisor exited"           '! kill -0 "$P" 2>/dev/null'
check "final state is stopped"      '[ "$(state t1 state)" = "stopped" ]'

echo "== 6. fairness: refuse a task that cannot finish =="
# MIN_TASK_TIME is read by the SUPERVISOR at startup, so it has to be set on the
# supervisor -- setting it on the slotctl invocation does nothing, which is exactly
# how the first version of this test fooled itself into passing nothing.
P6=$(SLOT_MIN_TASK_TIME=100000 sup t6 900); sleep 5
ctl run t6 scripts/escapes.sh --label doomed >/dev/null 2>&1; sleep 5
check "refusal recorded"            'grep -q refused "$SLOT_ROOT"/t6/done/*.json 2>/dev/null'
check "no task started"             '[ "$(state t6 task_running)" = "0" ]'
ctl abort t6 >/dev/null 2>&1; sleep 4

echo "== 8. lifetime is clamped to the ceiling =="
P2=$(sup t2 999999); sleep 5
check "clamped to 24h"              '[ "$(state t2 max_lifetime_s)" = "86400" ]'
ctl abort t2 >/dev/null 2>&1; sleep 5
check "abort released the slot"     '! kill -0 "$P2" 2>/dev/null'

echo "== 9. idle cap releases the slot =="
SLOT_MAX_IDLE=8 P3=$(SLOT_MAX_IDLE=8 sup t3 900); sleep 20
check "released on idle cap"        '[ "$(state t3 state)" = "released-idle" ] && ! kill -0 "$P3" 2>/dev/null'

echo "== 10. pre-queue runs on first poll =="
ctl run t4 scripts/escapes.sh --label prequeued --queue >/dev/null 2>&1
check "queued before slot exists"   '[ -f "$SLOT_ROOT/t4/inbox/000001.json" ]'
P4=$(sup t4 900); sleep 8
check "ran without further input"   '[ "$(state t4 task_label)" = "prequeued" ]'
ctl abort t4 >/dev/null 2>&1; sleep 6

for p in $P $P2 $P3 $P4 ${P6:-}; do kill -9 "$p" 2>/dev/null; done
while read -r p; do [ -n "$p" ] && kill -9 "$p" 2>/dev/null; done < "$TMP/gpu_holders"
pkill -9 -P $$ 2>/dev/null
rm -rf "$TMP"

echo
echo "passed $PASS, failed $FAIL"
[ "$FAIL" -eq 0 ]
