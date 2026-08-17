#!/bin/bash
# slotmeta -- Slurm slot. Submit with `sbatch run_slot.slurm`, or via
# `slotctl`-adjacent tooling. The supervisor clamps its own lifetime to the
# smaller of SLOT_MAX_LIFETIME and the walltime Slurm actually granted.
#SBATCH --job-name=slot
#SBATCH --time=18:00:00
#SBATCH --nodes=1
#SBATCH --gpus-per-node=4
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G
#SBATCH --output=logs/slot_%j.out
#SBATCH --error=logs/slot_%j.err
set -uo pipefail

# SLOT_ROOT must be visible from both the login node and the compute node.
export SLOT_ROOT="${SLOT_ROOT:-$HOME/slots}"
export SLOT_REPO="${SLOT_REPO:-$PWD}"
export SLOT_SNAPSHOT_EXTRA="${SLOT_SNAPSHOT_EXTRA:-}"

HERE="$(cd "$(dirname "$0")/.." && pwd)"
exec bash "$HERE/slot_supervisor.sh" "${SLOT_NAME:-slot-$SLURM_JOB_ID}" "${SLOT_MAX_LIFETIME:-43200}"
