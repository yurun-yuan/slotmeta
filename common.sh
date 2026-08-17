#!/bin/bash
# slotmeta -- scheduler-agnostic supervisor core.
#
# Sourced by slot_supervisor.sh AFTER an adapter (adapters/condor.sh or
# adapters/slurm.sh). Nothing in this file may reference a scheduler directly: if
# you need to know the job id, the walltime, or how to recognise our processes,
# call the adapter. See README.md for the contract.
#
# NOT `set -e`: a task failing is normal and must never take the supervisor down
# with it -- that would throw away exactly the walltime this exists to preserve.
set -uo pipefail
# Deliberately NOT `set -m`, and this is load-bearing. Job control makes every
# background job a process-group leader, and setsid(1) FORKS when its caller is
# already a group leader instead of exec'ing in place. `$!` then names the
# short-lived setsid wrapper rather than the task: the supervisor watches the
# wrapper exit, concludes the task finished cleanly, and never tears anything down
# -- so a swap silently leaves the old task running alongside the new one. Caught by
# tests/run_tests.sh case 4. Tasks get their own process group from setsid itself,
# so job control buys nothing here, and start_task reads the pid back from the task
# rather than trusting `$!` at all.

# ---------------------------------------------------------------- configuration
SLOT_ROOT="${SLOT_ROOT:?SLOT_ROOT must be set to a directory both sides can see}"
AGENT="${AGENT:?AGENT (slot name) must be set}"
DIR="$SLOT_ROOT/$AGENT"

# Fairness caps. A slot that outlives its usefulness is squatting on a resource
# other people are queued for, so the supervisor enforces these itself and exits
# -- releasing the allocation -- when any binds. Read README.md before raising one.
CEILING="${SLOT_CEILING:-86400}"                  # 24h; no configuration may exceed this
MAX_LIFETIME="${SLOT_MAX_LIFETIME:-43200}"        # 12h default
MAX_IDLE="${SLOT_MAX_IDLE:-1800}"                 # 30min with no task -> release
MIN_TASK_TIME="${SLOT_MIN_TASK_TIME:-900}"        # refuse a task that cannot get a useful run
POLL="${SLOT_POLL:-15}"
GRACE="${SLOT_KILL_GRACE:-90}"                    # SIGTERM -> SIGKILL window
SWEEP_ROUNDS="${SLOT_SWEEP_ROUNDS:-5}"            # straggler sweep convergence cap

[ "$MAX_LIFETIME" -gt "$CEILING" ] 2>/dev/null && MAX_LIFETIME=$CEILING
[ "$MAX_LIFETIME" -lt 600 ] 2>/dev/null && MAX_LIFETIME=600

# The scheduler may impose a shorter walltime than our own cap. Take the minimum,
# so the fairness cap can only ever shorten a slot, never extend it past what the
# scheduler granted.
_sched_left="$(sched_walltime_left_s)"
case "$_sched_left" in
  ''|*[!0-9]*) : ;;
  *) [ "$_sched_left" -lt "$MAX_LIFETIME" ] && MAX_LIFETIME="$_sched_left" ;;
esac

START=$(date +%s)
DEADLINE=$(( START + MAX_LIFETIME ))
JOB_ID="$(sched_job_id)"
TAG_VAR="$(sched_job_tag_var)"
TAG_VAL="$(sched_job_tag_val)"

mkdir -p "$DIR"/{inbox,done,tasks,ctl}
ALOG="$DIR/supervisor.log"
log() { echo "[$(date -Is)] $*" >> "$ALOG"; echo "[$(date -Is)] $*"; }

# ------------------------------------------------------------------- /proc utils
# /proc/<pid>/stat's comm field can contain spaces and parens, so everything is
# parsed relative to the LAST ')'. In that tail, field 1 is state, 2 is ppid, and
# 20 is starttime (overall fields 3, 4 and 22).
#
# Fork-free on purpose. The obvious version -- $(cat ...) piped to awk -- costs two
# forks per pid, and the sweep walks every pid on the node: measured at 17.6 s for a
# single pass over 9216 processes, which turns a teardown into a hang. `read <` and
# array splitting are builtins, so the same pass costs no forks at all.
# Sets globals rather than echoing, because $(...) is itself a fork and the sweep
# calls this once per pid on the node. Echoing cost 6.6 s per pass over 9216 pids;
# this costs no forks at all.
PROC_PPID=""; PROC_ST=""
proc_read() {   # $1=pid -> PROC_PPID, PROC_ST; 1 if unreadable
  local line; local -a f
  { read -r line < "/proc/$1/stat"; } 2>/dev/null || return 1
  line=${line#*") "}
  f=($line)
  PROC_PPID=${f[1]}; PROC_ST=${f[19]}
  [ -n "$PROC_ST" ]
}
# Convenience wrappers for the non-hot paths, where one fork does not matter.
proc_ppid()      { proc_read "$1" && printf '%s\n' "$PROC_PPID"; }
proc_starttime() { proc_read "$1" && printf '%s\n' "$PROC_ST"; }

# Processes that must never be signalled: this supervisor and its whole ancestor
# chain. Computed once, at startup, before any task exists.
PROTECTED=" $$ "
_p=$(proc_ppid $$)
while [ -n "${_p:-}" ] && [ "$_p" -gt 1 ] 2>/dev/null; do
  PROTECTED="$PROTECTED$_p "
  _p=$(proc_ppid "$_p")
done
is_protected() { case "$PROTECTED" in *" $1 "*) return 0;; esac; return 1; }

# ------------------------------------------------------------------ task state
TASK_PID=""; TASK_SEQ=""; TASK_LABEL=""; TASK_PORT=""; TASK_DIR=""
TASK_START=0; TASK_START_TICKS=0
LAST_SEQ=0
IDLE_SINCE=$(date +%s)
GPU_DIRTY=0

# ------------------------------------------------------------------- teardown
# Three layers, because Ray defeats any one of them:
#
#   1. process group   catches most children. Misses anything that calls setsid()
#                      -- `ray start --head` daemonises, so raylet and gcs_server
#                      leave the group -- and misses daemons reparented to init
#                      when the driver exits.
#   2. tagged sweep    every process carrying OUR job tag in /proc/<pid>/environ,
#                      started at or after the task, not protected. Another user's
#                      environ is unreadable, so they are unreachable by
#                      construction; matching the tag also spares our OWN other
#                      jobs on the same node.
#   3. GPU backstop    whatever still holds GPU memory, whether or not we can
#                      explain how it survived. This is the only test that
#                      actually decides whether the next task can start.
#
# NEVER `ray stop` (nodes are not PID-isolated -- it kills co-tenants) and NEVER
# "kill everything owned by $USER" (kills this supervisor, and kills our own other
# job when two share a node).

# Spare a candidate if, walking up, we find an ancestor older than the task BEFORE
# the chain leaves our tree. Leaving the tree means reaching init or scheduler
# infrastructure -- reparented orphans land there and those are older than every
# task, so treating them as protectors would shield exactly the daemons we hunt.
protected_by_ancestry() {
  local pid=$1 task_ticks=$2 hops=0 st
  while [ -n "$pid" ] && [ "$pid" -gt 1 ] 2>/dev/null && [ $hops -lt 40 ]; do
    is_protected "$pid" && return 0
    sched_is_infra_pid "$pid" && return 1        # left our tree
    proc_read "$pid" || return 1                 # globals, not $(...): no fork per hop
    st=$PROC_ST
    [ "$st" -lt "$task_ticks" ] 2>/dev/null && return 0
    pid=$PROC_PPID; hops=$((hops+1))
  done
  return 1
}

collect_victims() {   # task_start_ticks -> pids, one per line
  local task_ticks=$1 pid env st
  [ -z "$TAG_VAL" ] && return 0

  # Filter order is a performance decision, not a stylistic one. A shared login node
  # can carry ~9000 processes, and reading /proc/<pid>/environ for all of them --
  # five rounds, twice per round -- takes minutes and effectively hangs a teardown
  # (measured). Cheap, highly selective tests run first, so environ is read only for
  # the handful of our own processes younger than the task.
  for d in /proc/[0-9]*; do
    pid=${d#/proc/}
    [ "$pid" = "$BASHPID" ] && continue          # the scanning subshell itself
    is_protected "$pid" && continue
    # 1. started at or after the task. Eliminates essentially every pre-existing
    #    process on the node, at one builtin read each and NO fork.
    proc_read "$pid" || continue
    [ "$PROC_ST" -ge "$task_ticks" ] 2>/dev/null || continue
    # 2. ours. `-O` is a bash builtin ("owned by the effective uid"), so this costs
    #    no fork -- unlike `stat -c %u`, which costs one per pid.
    [ -O "$d" ] || continue
    sched_is_infra_pid "$pid" && continue
    # 3. this allocation's tag. environ is readable only for our own processes, so
    #    other users are unreachable by construction; the tag also excludes our OWN
    #    other jobs on the same node.
    env=$(tr '\0' '\n' < "$d/environ" 2>/dev/null | grep -m1 "^${TAG_VAR}=") || continue
    [ "${env#*=}" = "$TAG_VAL" ] || continue
    protected_by_ancestry "$pid" "$task_ticks" && continue
    echo "$pid"
  done
}

# One pass is not enough: the victim list is a snapshot and raylet respawns
# workers, so a worker forked between scan and kill survives still holding GPU
# memory. Rescan until a pass comes back empty.
sweep_stragglers() {
  local task_ticks=$1 round pids n began; began=$(date +%s)
  for round in $(seq 1 "$SWEEP_ROUNDS"); do
    # A teardown must be bounded. Whatever the sweep has not caught by now, the GPU
    # backstop still has to clear before the next task starts, and that check is the
    # one that actually gates progress.
    if [ $(( $(date +%s) - began )) -gt "${SLOT_SWEEP_BUDGET_S:-120}" ]; then
      log "  sweep budget exhausted after $(( $(date +%s) - began ))s; leaving the rest to the GPU backstop"
      return 1
    fi
    mapfile -t pids < <(collect_victims "$task_ticks")
    n=${#pids[@]}
    [ "$n" -eq 0 ] && { [ "$round" -gt 1 ] && log "  sweep converged after $((round-1)) pass(es)"; return 0; }
    log "  sweep pass $round: $n straggler(s): ${pids[*]}"
    kill -TERM "${pids[@]}" 2>/dev/null
    sleep 3
    mapfile -t pids < <(collect_victims "$task_ticks")
    [ ${#pids[@]} -gt 0 ] && { kill -KILL "${pids[@]}" 2>/dev/null; sleep 2; }
  done
  mapfile -t pids < <(collect_victims "$task_ticks")
  [ ${#pids[@]} -eq 0 ] && return 0
  log "  *** sweep DID NOT CONVERGE after $SWEEP_ROUNDS passes: ${pids[*]}"
  return 1
}

gpu_pids() {   # ours, unprotected, currently holding GPU memory
  local out="" p uid
  for p in $(nvidia-smi --query-compute-apps=pid --format=csv,noheader 2>/dev/null | tr -d ' '); do
    case "$p" in ''|*[!0-9]*) continue;; esac
    is_protected "$p" && continue
    uid=$(stat -c %u "/proc/$p" 2>/dev/null) || continue
    [ "$uid" = "$(id -u)" ] && out="$out $p"
  done
  echo "$out"
}

reap_gpu_holders() {
  local round pids
  for round in 1 2 3; do
    pids=$(gpu_pids); [ -z "$pids" ] && return 0
    log "  GPU still held by:$pids (round $round)"
    for p in $pids; do
      log "    pid $p: $(tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null | cut -c1-110)"
    done
    kill -TERM $pids 2>/dev/null
    local w=0
    while [ -n "$(gpu_pids)" ] && [ $w -lt 30 ]; do sleep 3; w=$((w+3)); done
    pids=$(gpu_pids); [ -z "$pids" ] && return 0
    log "  SIGKILL:$pids"; kill -KILL $pids 2>/dev/null; sleep 5
  done
  [ -z "$(gpu_pids)" ] && return 0
  log "  GPUs STILL HELD after three rounds:$(gpu_pids)"
  return 1
}

kill_task() {
  [ -z "$TASK_PID" ] && return 0
  local why="${1:-swap}"
  log "tearing down task $TASK_SEQ (${TASK_LABEL:-?}) pid=$TASK_PID reason=$why"

  kill -TERM "-$TASK_PID" 2>/dev/null            # layer 1: process group
  local waited=0
  while kill -0 "-$TASK_PID" 2>/dev/null && [ $waited -lt $GRACE ]; do sleep 3; waited=$((waited+3)); done
  kill -0 "-$TASK_PID" 2>/dev/null && { log "  group alive after ${GRACE}s, SIGKILL"; kill -KILL "-$TASK_PID" 2>/dev/null; sleep 3; }

  sweep_stragglers "$TASK_START_TICKS"           # layer 2: tagged sweep

  if reap_gpu_holders; then                      # layer 3: GPU backstop
    GPU_DIRTY=0; log "  GPUs clear"
  else
    GPU_DIRTY=1; log "  *** GPUs NOT CLEAN -- refusing further tasks"
  fi
  [ -n "$TASK_PORT" ] && rm -rf "/tmp/r$TASK_PORT" 2>/dev/null
  echo "killed:$why" > "$TASK_DIR/exit" 2>/dev/null
  TASK_PID=""; TASK_SEQ=""; TASK_LABEL=""; TASK_PORT=""; TASK_DIR=""
  IDLE_SINCE=$(date +%s)
}

# ---------------------------------------------------------------------- state
write_state() {
  local st="$1" now running=0; now=$(date +%s)
  [ -n "$TASK_PID" ] && running=1
  cat > "$DIR/state.json.tmp" <<EOF
{"agent": "$AGENT", "scheduler": "$(sched_name)", "job_id": "$JOB_ID",
 "host": "$(hostname)", "state": "$st",
 "now": "$(date -Is)", "now_epoch": $now, "uptime_s": $(( now - START )),
 "remaining_s": $(( DEADLINE - now )), "deadline_epoch": $DEADLINE,
 "max_lifetime_s": $MAX_LIFETIME, "max_idle_s": $MAX_IDLE,
 "task_running": $running, "task_seq": "${TASK_SEQ:-}", "task_label": "${TASK_LABEL:-}",
 "task_port": "${TASK_PORT:-}",
 "task_uptime_s": $([ -n "$TASK_PID" ] && echo $(( now - TASK_START )) || echo 0),
 "idle_s": $([ -n "$TASK_PID" ] && echo 0 || echo $(( now - IDLE_SINCE ))),
 "last_seq": $LAST_SEQ, "gpu_holders": $(gpu_pids | wc -w), "gpu_dirty": $GPU_DIRTY}
EOF
  mv -f "$DIR/state.json.tmp" "$DIR/state.json"
}

finish_cmd() {   # seq status note
  python3 - "$DIR/inbox/$1.json" "$DIR/done/$1.json" "$2" "$3" <<'PY' 2>/dev/null || \
    mv -f "$DIR/inbox/$1.json" "$DIR/done/$1.json" 2>/dev/null
import json, sys, datetime
src, dst, status, note = sys.argv[1:5]
try:    d = json.load(open(src))
except Exception: d = {}
d["result"] = {"status": status, "note": note,
               "handled": datetime.datetime.now().astimezone().isoformat()}
json.dump(d, open(dst, "w"), indent=1)
PY
  rm -f "$DIR/inbox/$1.json"
}

# ----------------------------------------------------------------- task launch
# Freeze every script a task runs. Bash reads a script by BYTE OFFSET, so editing
# one while it executes makes the running shell resume at the wrong place --
# silently. That hazard is CONSTANT here, because the whole point of a slot is to
# keep working on the shared filesystem while a task runs.
snapshot_task() {
  local seq=$1 first=$2 snap="$DIR/tasks/$seq/snap"
  mkdir -p "$snap"
  cp -f "$first" "$snap/" 2>/dev/null || return 1
  for extra in $SLOT_SNAPSHOT_EXTRA; do [ -f "$extra" ] && cp -f "$extra" "$snap/" 2>/dev/null; done
  echo "$snap"
}

start_task() {   # seq script port label envjson
  local seq="$1" script="$2" port="$3" label="$4" envjson="$5"
  local now left; now=$(date +%s); left=$(( DEADLINE - now ))
  if [ "$left" -lt "$MIN_TASK_TIME" ]; then
    log "REFUSING task $seq: ${left}s left, below MIN_TASK_TIME=${MIN_TASK_TIME}s"
    finish_cmd "$seq" refused "only ${left}s before deadline"; return 1
  fi
  kill_task swap
  if [ "$GPU_DIRTY" = "1" ]; then
    log "REFUSING task $seq: GPUs still held; a run started now would OOM"
    finish_cmd "$seq" refused "GPUs not clean after teardown"; return 1
  fi

  TASK_DIR="$DIR/tasks/$seq"; mkdir -p "$TASK_DIR"
  cp -f "$DIR/inbox/$seq.json" "$TASK_DIR/cmd.json" 2>/dev/null

  local first="${script%% *}" snap
  if [ -f "$first" ] && snap=$(snapshot_task "$seq" "$first"); then
    script="$snap/$(basename "$first")${script#"$first"}"
    log "  snapshotted $(basename "$first") to snap/"
  else
    log "  WARNING: '$first' not a file; running unsnapshotted"
    snap=""
  fi

  # Per-task env. Keys filtered to [A-Za-z_][A-Za-z0-9_]* so a command file cannot
  # inject shell, and values are json-quoted rather than eval'd.
  local envfile="$TASK_DIR/env.sh"; : > "$envfile"
  if [ -n "$envjson" ] && [ "$envjson" != "null" ] && [ "$envjson" != "{}" ]; then
    python3 - "$envjson" "$envfile" <<'PY'
import json, re, sys
try:    d = json.loads(sys.argv[1])
except Exception: d = {}
with open(sys.argv[2], "w") as f:
    for k, v in (d or {}).items():
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(k)):
            f.write("export %s=%s\n" % (k, json.dumps(str(v))))
PY
  fi

  log "starting task $seq: $script ${label:+($label)} port=${port:-none}"
  rm -f "$TASK_DIR/pid" "$TASK_DIR/rc"
  # The task reports its OWN pid and its own exit code. Neither `$!` nor `wait` is
  # trusted: depending on whether setsid forks, `$!` may name a wrapper that exits
  # immediately, and a forked task is not our child so `wait` cannot see it.
  setsid bash -c "echo \$\$ > '$TASK_DIR/pid'; \
                  cd '${SLOT_WORKDIR:-$PWD}'; \
                  ${snap:+export SLOT_SNAP='$snap';} \
                  . '$envfile' 2>/dev/null; \
                  $script; echo \$? > '$TASK_DIR/rc'" \
    > "$TASK_DIR/task.log" 2>&1 &
  local waited=0
  while [ ! -s "$TASK_DIR/pid" ] && [ $waited -lt 20 ]; do sleep 0.5; waited=$((waited+1)); done
  TASK_PID=$(cat "$TASK_DIR/pid" 2>/dev/null)
  if [ -z "$TASK_PID" ]; then
    log "  task $seq never reported a pid; treating as failed to start"
    finish_cmd "$seq" error "task did not report a pid"
    TASK_DIR=""; return 1
  fi
  TASK_START=$(date +%s)
  TASK_START_TICKS=$(proc_starttime "$TASK_PID")
  TASK_SEQ="$seq"; TASK_LABEL="$label"; TASK_PORT="$port"
  finish_cmd "$seq" started "pid=$TASK_PID"
  return 0
}

reap_task() {
  [ -z "$TASK_PID" ] && return 0
  kill -0 "$TASK_PID" 2>/dev/null && return 0
  # Read the exit code the task wrote for itself. `wait` is unusable here: when
  # setsid forks, the task is not our child.
  local rc; rc=$(cat "$TASK_DIR/rc" 2>/dev/null); rc=${rc:-unknown}
  log "task $TASK_SEQ exited rc=$rc after $(( $(date +%s) - TASK_START ))s"
  echo "exited:$rc" > "$TASK_DIR/exit" 2>/dev/null
  # A driver exiting on its own still leaves daemons behind; same cleanup, same
  # verification, because the next task cares either way.
  sweep_stragglers "$TASK_START_TICKS"
  reap_gpu_holders || { GPU_DIRTY=1; log "  *** GPUs NOT CLEAN after task exit"; }
  [ -n "$TASK_PORT" ] && rm -rf "/tmp/r$TASK_PORT" 2>/dev/null
  TASK_PID=""; TASK_SEQ=""; TASK_LABEL=""; TASK_PORT=""; TASK_DIR=""
  IDLE_SINCE=$(date +%s)
}

# --------------------------------------------------------------------- signals
on_term() { log "SIGTERM (eviction or stop)"; kill_task sigterm; write_state stopping; exit 0; }
trap on_term TERM INT

# ------------------------------------------------------------------- main loop
supervise() {
  log "slot '$AGENT' up: scheduler=$(sched_name) job=$JOB_ID host=$(hostname)"
  log "  lifetime ${MAX_LIFETIME}s (deadline $(date -Is -d @$DEADLINE)), idle cap ${MAX_IDLE}s"
  log "  identifying our processes by $TAG_VAR=${TAG_VAL:-<unset!>}"
  [ -z "$TAG_VAL" ] && log "  WARNING: job tag empty -- the straggler sweep is disabled, GPU backstop only"
  write_state idle

  while :; do
    local now; now=$(date +%s)

    if [ "$now" -ge "$DEADLINE" ]; then
      log "LIFETIME REACHED (${MAX_LIFETIME}s). Releasing the allocation."
      kill_task deadline; write_state expired; return 0
    fi
    reap_task
    if [ -z "$TASK_PID" ] && [ $(( now - IDLE_SINCE )) -ge "$MAX_IDLE" ]; then
      log "IDLE $(( now - IDLE_SINCE ))s >= ${MAX_IDLE}s with no work. Releasing the allocation."
      write_state released-idle; return 0
    fi

    # Immediate signals, checked before queued commands so `abort` cannot be stuck
    # behind a long queue.
    if [ -f "$DIR/ctl/abort" ]; then
      rm -f "$DIR/ctl/abort"; log "ABORT requested"
      kill_task abort; write_state stopped; return 0
    fi
    if [ -f "$DIR/ctl/drain" ]; then
      rm -f "$DIR/ctl/drain"; log "DRAIN requested: releasing after the current task"
      DRAINING=1
    fi

    for f in $(ls "$DIR/inbox"/*.json 2>/dev/null | sort); do
      local seq; seq=$(basename "$f" .json)
      [ "$((10#$seq))" -le "$LAST_SEQ" ] && { rm -f "$f"; continue; }
      local CMDF="$DIR/.cmd.$seq.sh"
      SLOT_REPO="$SLOT_REPO" python3 - "$f" > "$CMDF" <<'PY'
import json, os, shlex, sys
base = os.environ.get("SLOT_REPO", "")
try:    d = json.load(open(sys.argv[1]))
except Exception: d = {}
script = str(d.get("script", "") or "")
if script and not script.startswith("/") and base:
    script = os.path.join(base, script)
parts = [script] + [str(a) for a in (d.get("args") or [])]
cmd = " ".join(shlex.quote(p) for p in parts if p != "")
print("CMD_OP=%s"     % shlex.quote(str(d.get("op", "noop"))))
print("CMD_SCRIPT=%s" % shlex.quote(cmd))
print("CMD_PORT=%s"   % shlex.quote(str(d.get("port", "") or "")))
print("CMD_LABEL=%s"  % shlex.quote(str(d.get("label", "") or "")))
print("CMD_ENV=%s"    % shlex.quote(json.dumps(d.get("env") or {})))
PY
      CMD_OP=""; CMD_SCRIPT=""; CMD_PORT=""; CMD_LABEL=""; CMD_ENV=""
      . "$CMDF" 2>/dev/null; rm -f "$CMDF"
      LAST_SEQ=$((10#$seq))
      log "command $seq: op=${CMD_OP:-noop} ${CMD_LABEL:+label=$CMD_LABEL}"
      case "${CMD_OP:-noop}" in
        run)    start_task "$seq" "$CMD_SCRIPT" "$CMD_PORT" "$CMD_LABEL" "$CMD_ENV" ;;
        idle|cancel)
                kill_task "$CMD_OP"; finish_cmd "$seq" ok "task cancelled; slot held" ;;
        drain)  DRAINING=1; finish_cmd "$seq" ok "will release after the current task" ;;
        stop)   kill_task stop; finish_cmd "$seq" ok "slot released"
                write_state stopped; return 0 ;;
        *)      finish_cmd "$seq" error "unknown op '${CMD_OP:-}'" ;;
      esac
      write_state "$([ -n "$TASK_PID" ] && echo running || echo idle)"
    done

    if [ "${DRAINING:-0}" = "1" ] && [ -z "$TASK_PID" ]; then
      log "drained: no task running. Releasing the allocation."
      write_state drained; return 0
    fi

    write_state "$([ -n "$TASK_PID" ] && echo running || echo idle)"
    sleep "$POLL"
  done
}
