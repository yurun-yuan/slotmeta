#!/bin/bash
# slotmeta -- slot supervisor entry point.
#
#   slot_supervisor.sh <slot-name> [max-lifetime-seconds]
#
# Submitted as the payload of a scheduler job (see templates/). It picks an adapter,
# loads the scheduler-agnostic core, and runs the supervise loop. Everything
# interesting lives in common.sh; this file only wires things together.
#
# Environment:
#   SLOT_ROOT              control directory both the submit host and the compute
#                          node can see. REQUIRED.
#   SLOT_SCHEDULER         condor | slurm. Auto-detected when unset.
#   SLOT_REPO              base for relative script paths in commands.
#   SLOT_WORKDIR           cwd for tasks (default: the supervisor's cwd).
#   SLOT_SNAPSHOT_EXTRA    extra files to freeze alongside each task's script,
#                          e.g. a sourced env setup. Space-separated.
#   SLOT_MAX_IDLE, SLOT_MIN_TASK_TIME, SLOT_POLL, SLOT_KILL_GRACE, SLOT_CEILING
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AGENT="${1:?usage: slot_supervisor.sh <slot-name> [max-lifetime-seconds]}"
export AGENT
export SLOT_MAX_LIFETIME="${2:-${SLOT_MAX_LIFETIME:-43200}}"
export SLOT_SNAPSHOT_EXTRA="${SLOT_SNAPSHOT_EXTRA:-}"
export SLOT_REPO="${SLOT_REPO:-}"

# Adapter selection. Explicit beats detection; detection beats guessing.
SCHED="${SLOT_SCHEDULER:-}"
if [ -z "$SCHED" ]; then
  if   [ -n "${SLURM_JOB_ID:-}" ];          then SCHED=slurm
  elif [ -n "${_CONDOR_SCRATCH_DIR:-}" ];   then SCHED=condor
  elif command -v squeue     >/dev/null 2>&1; then SCHED=slurm
  elif command -v condor_q   >/dev/null 2>&1; then SCHED=condor
  else
    echo "cannot determine scheduler; set SLOT_SCHEDULER=condor|slurm" >&2; exit 2
  fi
fi
ADAPTER="$HERE/adapters/$SCHED.sh"
[ -f "$ADAPTER" ] || { echo "no adapter for '$SCHED' at $ADAPTER" >&2; exit 2; }

# shellcheck source=/dev/null
. "$ADAPTER"
# Fail fast and loudly if an adapter is incomplete, rather than discovering a
# missing function mid-teardown with a task running.
for fn in sched_name sched_job_id sched_walltime_left_s sched_job_tag_var \
          sched_job_tag_val sched_is_infra_pid; do
  declare -F "$fn" >/dev/null || { echo "adapter '$SCHED' is missing $fn()" >&2; exit 2; }
done

# shellcheck source=/dev/null
. "$HERE/common.sh"

supervise
