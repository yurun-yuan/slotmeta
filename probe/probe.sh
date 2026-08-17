#!/bin/bash
# Lightweight probe task: proves the slot works end to end on a real execute node,
# and gives the teardown something that genuinely escapes the process group the way
# ray's daemons do (setsid + parent exits -> reparented to init).
echo "=== probe starting $(date -Is) ==="
echo "host      : $(hostname)"
echo "pwd       : $(pwd)"
echo "snap dir  : ${SLOT_SNAP:-<none>}"
echo "job tag   : _CONDOR_SCRATCH_DIR=${_CONDOR_SCRATCH_DIR:-<unset>}"
echo "gpus      :"; nvidia-smi --query-gpu=index,name,memory.total --format=csv 2>&1 | sed 's/^/            /'
echo "staging   : $(ls /staging/yyuan244/slotmeta >/dev/null 2>&1 && echo readable || echo UNREADABLE)"

# An escaping grandchild: own session, and its parent exits immediately, so it is
# reparented to init. A process-group kill cannot reach it -- the tagged sweep must.
setsid bash -c 'exec sleep 3600' &
sleep 1
echo "escapee   : spawned (pgid $(ps -o pgid= -p $(pgrep -n -u $(id -u) -f "sleep 3600") 2>/dev/null | tr -d ' ') vs mine $(ps -o pgid= -p $$ | tr -d ' '))"

trap 'echo "probe got SIGTERM at $(date -Is)"; exit 143' TERM
i=0
while :; do i=$((i+1)); echo "probe tick $i $(date +%T)"; sleep 10; done
