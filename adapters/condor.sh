#!/bin/bash
# Scheduler adapter: HTCondor (UW-Madison CHTC).
#
# Every function here is part of the adapter contract documented in meta/README.md.
# meta/common.sh calls these and nothing else scheduler-specific; if you find a
# `condor_*` command or a `_CONDOR_*` variable outside this file, that is a bug.

sched_name() { echo condor; }

# Identity of this allocation. Condor exports the job ad path and a scratch dir;
# the cluster id is in the ad. Fall back to the scratch dir, which is unique per
# job and is what we match processes by anyway.
sched_job_id() {
  local id=""
  [ -n "${_CONDOR_JOB_AD:-}" ] && [ -r "${_CONDOR_JOB_AD}" ] && \
    id=$(awk -F' = ' '/^ClusterId /{print $2}' "$_CONDOR_JOB_AD" 2>/dev/null | tr -d ' ')
  [ -z "$id" ] && id=$(basename "${_CONDOR_SCRATCH_DIR:-unknown}")
  echo "${id:-unknown}"
}

# Condor has no walltime we can read from inside the job the way Slurm does:
# JobRuntimeGuarantee is a machine attribute, not a per-job countdown, and the AP
# commands are unavailable on the execute node. So the deadline is entirely the
# supervisor's own MAX_LIFETIME. Returning empty tells common.sh to use it.
sched_walltime_left_s() { echo ""; }

# How we recognise OUR processes in /proc/<pid>/environ. Condor gives every process
# in the job _CONDOR_SCRATCH_DIR; it is unique per allocation, so another user's
# processes never match (and their environ is unreadable anyway), and neither do our
# OWN other jobs on the same node -- which matters, because that has happened here.
sched_job_tag_var() { echo "_CONDOR_SCRATCH_DIR"; }
sched_job_tag_val() { echo "${_CONDOR_SCRATCH_DIR:-}"; }

# Scheduler infrastructure that must never be signalled, and must never be treated
# as an "older than the task" protector -- reparented orphans land under these, and
# protecting their descendants would shield exactly the Ray daemons we are hunting.
sched_is_infra_pid() {
  # Fork-free: $(cat ...) would cost a subprocess per candidate, and the
  # straggler sweep calls this in a loop over /proc.
  local comm
  { read -r comm < "/proc/$1/comm"; } 2>/dev/null || return 1
  case "$comm" in condor_starter|condor_exec.exe|condor_master|systemd|init) return 0;; esac
  return 1
}

# --- access-point side (used by slotctl, never on the execute node) ----------
sched_submit()    { condor_submit "$@"; }
sched_cancel()    { condor_rm "$@"; }
sched_list_live() { condor_q -af ClusterId JobStatus 2>/dev/null; }
