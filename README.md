# slotmeta

**Reuse an allocation's leftover walltime for a different job.**

Cancelling a run with hours left on its allocation hands those hours back to the
scheduler, and the next thing you want to run starts from the back of the queue. On a
contended partition that is measured in hours — on one CHTC pool, only *three* slots
can host a 4-GPU job at all, and a released one was reclaimed by another user **within
seconds**, costing a five-hour wait for a restart whose only purpose was picking up new
metrics.

A **slot** is an allocation whose payload is a supervisor loop polling a control
directory on shared storage, instead of one fixed workload. Swapping what runs is then
a file write rather than a re-queue.

```
submit host                     shared storage              compute node
-----------                     --------------              ------------
slotctl run slotA train.sh -->  $SLOT_ROOT/slotA/inbox/ <--  slot_supervisor.sh
                                $SLOT_ROOT/slotA/state.json  (polls every 15 s)
```

No daemon, no ssh, no socket: the only channel is a filesystem both sides already
share, so there is nothing to keep alive and nothing to reconnect.

Works on **HTCondor** and **Slurm** through a thin adapter. Everything else is shared.

## Quick start

```bash
export SLOT_ROOT=/staging/$USER/slots      # visible from BOTH sides
export SLOT_REPO=/staging/$USER/myrepo     # base for relative script paths

condor_submit templates/run_slot.sub       # or: sbatch templates/run_slot.slurm

./slotctl list
./slotctl run  slotA scripts/train.sh mycfg 6435 --label run-a
./slotctl swap slotA scripts/train.sh othercfg 6436 --label run-b   # ~30 s, same allocation
./slotctl log  slotA -f
./slotctl drain slotA                      # release once the current task finishes
```

`--queue` pre-queues for a slot that has not started yet, so work begins on the
supervisor's first poll rather than waiting for someone to notice the job landed.

## Commands

| command | effect |
|---|---|
| `list` | one line per slot: live, scheduler, state, task, walltime left, idle |
| `status <slot>` | detail, including a stale-heartbeat warning if the supervisor died |
| `run <slot> <script> [args...]` | run a script, replacing any current task |
| `swap` | alias for `run`; reads as intent when replacing work |
| `idle <slot>` | stop the task, **keep** the slot |
| `drain <slot>` | release once the current task finishes |
| `stop <slot>` | stop the task and release now |
| `abort <slot>` | release immediately, skipping the command queue |
| `log <slot> [-f]` / `slog <slot> [-f]` | tail the task log / the supervisor log |
| `queue <slot>` / `history <slot>` | pending / handled commands |

Options on `run`: `--label`, `--port`, `--env K=V` (repeatable), `--no-wait`, `--queue`.

## Fairness

Holding a scarce allocation you are not using is squatting. The supervisor enforces
these itself and **exits, releasing the allocation**, when any binds. They are
supervisor-side on purpose: nothing you send from the submit host can bypass them.

| cap | default | env | behaviour |
|---|---|---|---|
| `MAX_LIFETIME` | 12 h | `SLOT_MAX_LIFETIME` | total occupancy, regardless of activity |
| `CEILING` | **24 h** | `SLOT_CEILING` | clamps the above; a typo cannot create an indefinite reservation |
| `MAX_IDLE` | 30 min | `SLOT_MAX_IDLE` | no task running for this long → release. The anti-squat rule |
| `MIN_TASK_TIME` | 15 min | `SLOT_MIN_TASK_TIME` | refuse a task that cannot get a useful run before the deadline |

Where the scheduler grants a shorter walltime than `MAX_LIFETIME` (Slurm does; Condor
generally does not expose one from inside the job), the supervisor takes the
**minimum** — the cap can only ever shorten a slot, never extend it.

Raise a limit only with a reason you would be comfortable stating to the other groups
queued for the same hardware.

## Terminating a task, and why it takes three layers

This is the part that is easy to get wrong. Ray is the motivating case: `ray start
--head` daemonises, so `raylet` and `gcs_server` `setsid()` out of the task's process
group and keep holding GPU memory. A group kill alone leaves the next task to OOM ten
minutes later with an error that looks like something else entirely.

1. **Process group.** Every task runs under `setsid`, so it owns its group; teardown
   sends `SIGTERM` to the group, waits `SLOT_KILL_GRACE`, then `SIGKILL`. Misses
   anything that called `setsid()` itself, and misses daemons reparented to init when
   the driver exits — after reparenting, neither group membership nor ancestry finds
   them.

2. **Tagged sweep.** Every process carrying our job tag in `/proc/<pid>/environ`
   (`_CONDOR_SCRATCH_DIR` or `SLURM_JOB_ID`), started at or after the task, not
   protected. Three rules make this safe and convergent:
   - Another user's `environ` is unreadable, so **other users are unreachable by
     construction** — not by us guessing correctly.
   - Matching the tag also excludes **our own other jobs** on the same node, which is a
     real case: two jobs of the same user co-tenanting one node has happened.
   - One pass is not enough. `raylet` respawns workers, so a worker forked between
     scan and kill survives. The sweep rescans until a pass returns empty (capped, and
     it says so loudly if it never converges).
   - A process that predates the task is spared, **and so are its descendants** — they
     are all younger than the task, so without this an interactive debug shell would
     keep its shell and lose every command it ran. Walking up stops at init or
     scheduler infrastructure: reparented orphans land there, and treating those as
     protectors would shield exactly the daemons being hunted.

3. **GPU backstop.** Whatever still holds GPU memory, whether or not we can explain how
   it survived. This is the only test that actually decides whether the next task can
   start. If the GPUs cannot be cleared, the supervisor **refuses further tasks** and
   says so in `state.json` (`gpu_dirty`), rather than starting a run that will OOM.

**Never** `ray stop` — these nodes are not PID-isolated and it kills co-tenants'
clusters. **Never** "kill everything owned by `$USER`" — that kills the supervisor, and
kills your own other job on the same node. The supervisor and its entire ancestor chain
are in an explicit protected set that is computed at startup and never signalled.

## Editing scripts while a task runs

Bash reads a script by **byte offset**, so editing one while it executes makes the
running shell resume at the wrong place — silently, with arbitrary consequences. That
hazard is *constant* here, because the whole point of a slot is to keep working while a
task runs.

Every script is copied to `tasks/<seq>/snap/` at launch and **the copy is what
executes**. The shared filesystem stays editable, the next task picks the edit up, and
the running one is immune. Files the task *sources* (an env setup, say) are frozen
alongside it via `SLOT_SNAPSHOT_EXTRA`; `$SLOT_SNAP` is exported so the task can
reference its own snapshot directory.

## Control directory layout

```
$SLOT_ROOT/<slot>/
  inbox/NNNNNN.json    commands, written by slotctl, consumed in order, exactly once
  done/NNNNNN.json     the command plus its result
  ctl/{abort,drain}    immediate signals, checked before the queue
  tasks/NNNNNN/        cmd.json, task.log, pid, exit, env.sh, snap/
  state.json           heartbeat, rewritten every poll
  supervisor.log
```

`slotctl` writes `NNNNNN.json.tmp` and renames — atomic, so the supervisor never reads
a partial file. Liveness is decided from `state.json`'s **age**: scheduler removal and
eviction both leave the last state behind, so a file that exists proves nothing.

Command fields are read with a JSON parser and shell-quoted; nothing from a command
file is ever `eval`'d, and per-task env keys are filtered to
`[A-Za-z_][A-Za-z0-9_]*`.

## Writing an adapter

A scheduler adapter is one file in `adapters/` defining six functions. If you find a
scheduler-specific command anywhere else in this repo, that is a bug.

| function | returns |
|---|---|
| `sched_name` | `condor` \| `slurm` \| … |
| `sched_job_id` | this allocation's id |
| `sched_walltime_left_s` | seconds the scheduler still grants, or empty if unknowable from inside the job |
| `sched_job_tag_var` | env var name that identifies our processes in `/proc/<pid>/environ` |
| `sched_job_tag_val` | its value for this allocation |
| `sched_is_infra_pid <pid>` | true for scheduler infrastructure (`slurmstepd`, `condor_starter`, init) that must never be signalled and must never count as an ancestry protector |

Optionally, for submit-host tooling: `sched_submit`, `sched_cancel`, `sched_list_live`.

`slot_supervisor.sh` verifies the six required functions exist before starting, so an
incomplete adapter fails immediately instead of during a teardown with a task running.

## Tests

```bash
tests/run_tests.sh
```

No scheduler and no GPU needed: the protocol is filesystem-based, so the suite points
`SLOT_ROOT` at a scratch directory and stubs `nvidia-smi`. It drives the **real**
supervisor and the **real** slotctl through: startup, running a task, snapshot
isolation against a mid-run edit, hot swap, teardown of a process that escaped its
process group the way `raylet` does, the `MIN_TASK_TIME` refusal, `stop`, the 24 h
clamp, the idle release, and pre-queueing.

What the suite *cannot* cover is a real Ray cluster teardown; that is exercised by the
escaping-grandchild case, which reproduces the mechanism but not the scale.

## Status

Extracted from two independent implementations of the same idea — one for HTCondor, one
for Slurm — after they converged on the same protocol. The scheduler-specific parts are
the six adapter functions; everything else is shared.
