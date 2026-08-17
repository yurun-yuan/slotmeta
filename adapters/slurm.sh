#!/bin/bash
# Scheduler adapter: Slurm.
#
# Written to match the upstream async-rl meta/ implementation (ghx4), so a slot
# behaves the same on either cluster. Every function here is part of the adapter
# contract in meta/README.md; no `s*` Slurm command belongs outside this file.

sched_name() { echo slurm; }

sched_job_id() { echo "${SLURM_JOB_ID:-unknown}"; }

# Slurm DOES give a real allocation deadline, unlike condor. Prefer the env var
# (no fork, always present under sbatch); fall back to scontrol. common.sh takes
# the MINIMUM of this and its own MAX_LIFETIME, so the fairness cap can only ever
# make a slot shorter than its allocation, never longer.
sched_walltime_left_s() {
  local end now
  end="${SLURM_JOB_END_TIME:-}"
  if [ -z "$end" ] && command -v scontrol >/dev/null 2>&1; then
    end=$(scontrol show job "${SLURM_JOB_ID:-0}" 2>/dev/null |
          sed -n 's/.*EndTime=\([^ ]*\).*/\1/p' | head -1)
    [ -n "$end" ] && end=$(date -d "$end" +%s 2>/dev/null)
  fi
  case "$end" in ''|*[!0-9]*) echo ""; return;; esac
  now=$(date +%s)
  [ "$end" -gt "$now" ] && echo $(( end - now )) || echo 0
}

# SLURM_JOB_ID is inherited by every process in the step, so it identifies ours.
# Another user's /proc/<pid>/environ is unreadable, so they are unreachable by
# construction; matching the id also excludes our own other jobs on the node.
sched_job_tag_var() { echo "SLURM_JOB_ID"; }
sched_job_tag_val() { echo "${SLURM_JOB_ID:-}"; }

# slurmstepd is the step's parent. Reparented orphans land under it or init, and
# both predate every task -- treating them as protectors would shield exactly the
# Ray daemons the sweep exists to kill.
sched_is_infra_pid() {
  # Fork-free: $(cat ...) would cost a subprocess per candidate, and the
  # straggler sweep calls this in a loop over /proc.
  local comm
  { read -r comm < "/proc/$1/comm"; } 2>/dev/null || return 1
  case "$comm" in slurmstepd|slurmd|systemd|init) return 0;; esac
  return 1
}

# --- login-node side (used by slotctl) ---------------------------------------
sched_submit()    { sbatch "$@"; }
sched_cancel()    { scancel "$@"; }
sched_list_live() { squeue -h -u "$USER" -o '%i %T' 2>/dev/null; }
