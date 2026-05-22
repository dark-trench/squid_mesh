# Durable Dispatch Protocol

Squid Mesh's new runtime path treats dispatch state as an append-only journal.
The protocol and pure projection are storage-independent, and
`SquidMesh.Runtime.Journal` persists the same entries through `Jido.Storage`
thread journals and checkpoints.

## Threads

- Run thread: workflow lifecycle facts such as run start, planned runnables,
  applied runnable results, manual pause/resolution boundaries, and terminal
  status.
- Dispatch thread: runnable intent, claim, heartbeat, completion, failure, retry
  visibility, and live wakeup facts.
- Run index thread: rebuildable lookup entries for finding runs by workflow or
  host-facing keys.

`SquidMesh.Runtime.Journal` maps those logical threads to Jido thread IDs such
as `squid_mesh:run:<run-id>`, `squid_mesh:dispatch:<queue>`, and
`squid_mesh:run_index:<workflow>`. Runtime entries keep the Squid Mesh protocol
type as the Jido entry kind and store the protocol data as the entry payload, so
projections can be rebuilt from the thread after process restart.

## Jido.Storage Boundary

The journal boundary accepts any configured `Jido.Storage` adapter. It appends
entries with Jido's optimistic `:expected_rev` option, returns `{:error,
:conflict}` for stale appends, and stores projection checkpoints with the exact
Jido thread revision they cover. Checkpoints are rebuild accelerators; the
append-only thread remains the source of truth.

The storage-backed slices prove the Squid Mesh protocol can persist and restore
dispatch projections through `Jido.Storage`. The default live runtime still uses
the current host-executor and Postgres table path, while the temporary
`runtime: :journal` cutover path now writes run and dispatch facts through this
boundary.

For production adapters, the required storage properties are:

- ordered thread append with stable per-thread sequence numbers
- optimistic append conflict detection through `:expected_rev`
- checkpoint overwrite semantics for compact projections
- durable reload of thread entries and checkpoints after process restart

The Postgres path should use a `jido_ecto` adapter when that adapter provides
those properties. The Bedrock path should use a `jido_bedrock` adapter where
Bedrock is available. Squid Mesh should not introduce a second persistence
contract for those stores; adapters only need to satisfy `Jido.Storage`.

## Commit Order

Durable facts must be appended before live effects are treated as successful. A
worker wakeup is recoverable only when a matching runnable intent already exists
in the dispatch journal. If a wakeup is lost after the intent append, the
projection can still rediscover the visible attempt after restart. Duplicate
runnable intent entries are idempotent when their scheduled fields match;
conflicting entries for the same `runnable_key` are anomalies.

If a crash happens after the workflow run thread records planned runnables but
before the dispatch thread records matching `attempt_scheduled` entries, rebuilt
agents can recover through
`SquidMesh.Runtime.WorkflowAgent.schedule_pending_dispatches/4`. The workflow
agent derives planned-but-unscheduled runnables from the run projection, and the
dispatch agent appends the missing dispatch intents with its current dispatch
thread revision as the optimistic fence.

`SquidMesh.Runtime.AgentRecovery.recover/4` is the restart coordinator for the
current Jido-native agent slices. It rebuilds the run's workflow agent and the
queue's dispatch agent, schedules planned-but-missing dispatch intents first,
and only then applies completed dispatch results that are still missing from the
run thread.

For dependency-based workflows, Runic-ready runnables map to durable runnable
intent. Independent root steps may produce sibling runnable intents for the same
run, and a join step produces intent only after every dependency result has
already become durable. The dispatch protocol does not use host-job concurrency
as the source of truth for fan-out or fan-in readiness; persisted workflow facts
do.

## IntentLedger Alignment

Squid Mesh models workflow-specific facts, but its dispatch vocabulary is
compatible with IntentLedger's durable intent model and its `bedrock_job_queue`
runtime boundary:

- `attempt_scheduled` maps to an enqueued intent.
- `runnable_key` maps to the Squid workflow identity carried as an Intent key
  or lineage field.
- `step` and `input` map to intent kind and payload.
- `claim_id`, `claim_token_hash`, `owner_id`, and `lease_until` mirror
  the queue lease state behind IntentLedger.
- `attempt_heartbeat`, `attempt_completed`, and `attempt_failed` require the
  current claim fence.

Squid Mesh should integrate through a dispatch backend adapter rather than make
IntentLedger depend on Squid-specific workflow concepts. The adapter can map
Squid runnables to IntentLedger intents and translate lifecycle signals back
into the projection. IntentLedger is expected to be the default durable executor
path once that adapter is wired, but the Squid Mesh core protocol remains
backend-neutral so host applications can still provide their own dispatcher.

## Job Runner Boundary

The protocol does not assume Oban, Broadway, IntentLedger, or a custom process as
the delivery mechanism. A runner may wake, claim, execute, retry, or redeliver
work, but Squid Mesh treats the journal as authoritative for intent, claim,
lease, fencing, completion, failure, and retry visibility.

## Claims, Leases, And Heartbeats

Each attempt is fenced by `claim_id` and `claim_token_hash`. Workers hold the raw
claim token, but durable entries record only its hash. A heartbeat extends
`lease_until` only when it carries the current claim fence. Heartbeats from stale
claims are ignored by the projection and surfaced as anomalies. Expired claims
remain discoverable so a dispatch agent can redeliver work without relying on
in-memory state. A replacement claim is valid only after the prior lease has
expired; active claim takeover is an anomaly. Claims are valid only after the
attempt's `visible_at`, and heartbeat, completion, and failure facts are valid
only before the current lease expires.

`SquidMesh.Runtime.DispatchAgent.claim_next/4`,
`SquidMesh.Runtime.DispatchAgent.heartbeat/6`,
`SquidMesh.Runtime.DispatchAgent.complete/7`, and
`SquidMesh.Runtime.DispatchAgent.fail/7` are the current durable claim lifecycle
boundaries for the Jido-native runtime work. Claiming selects the next visible
or expired attempt from a rebuilt dispatch-agent projection and appends an
`attempt_claimed` entry with Jido's optimistic `:expected_rev` fence. Heartbeat,
completion, and failure appends validate the current claim fence before writing,
then append the matching lifecycle entry with the same optimistic thread fence.
On success, each API returns a lifecycle update map containing the updated
`:agent`, the lifecycle `:attempt`, and `:lease_until` for heartbeat calls. The
post-append projection is available at `agent.state.projection`; concurrent
stale callers receive `{:error, :conflict}` from the journal append.

IntentLedger is the intended future integration point for lease-backed
execution once its Bedrock and `bedrock_job_queue` recovery contracts settle.
Until then, Squid Mesh keeps the protocol dependency-free.

## Completion And Retry

Completion and failure entries must also carry the current claim fence.
Duplicate completion entries with the same claim and result are idempotent.
Conflicting or stale completions are ignored and reported as anomalies. Retry
scheduling is a durable fact with its own `visible_at`, so retry visibility
survives restart. A runnable result can be applied to the run thread only after
the matching completion is durable. `SquidMesh.Runtime.WorkflowAgent.apply_result/4`
is the current run-thread apply boundary: it accepts a completed dispatch
attempt, validates that the runnable belongs to the rebuilt workflow projection,
and appends `:runnable_applied` with an optimistic run-thread fence. Duplicate
application of an already-applied runnable is idempotent.

`SquidMesh.Runtime.WorkflowAgent.apply_pending_results/4` is the restart
recovery boundary for lost live wakeups: rebuilt workflow and dispatch agents
derive completed-but-unapplied attempts from their durable projections and append
the missing run-thread applications in order, using the latest run-thread fence
after each append.

## Manual Boundaries

Manual pause and approval states are run-thread facts, not dispatch-thread
facts. `manual_step_paused` records the current manual boundary with its step,
kind, timestamp, and persisted metadata. `manual_step_resolved` records that the
same boundary was completed by an operator action such as resume, approve, or
reject.

The workflow projection exposes only the current manual state. Duplicate pause
facts are idempotent when they match. A second active manual boundary, a stale
resolution, or a manual fact appended after the run is terminal becomes a
projection anomaly instead of changing the current state.

```mermaid
stateDiagram-v2
    [*] --> Running: run_started
    Running --> Paused: manual_step_paused
    Paused --> Running: manual_step_resolved
    Running --> Terminal: run_terminal
    Paused --> Terminal: run_terminal

    Paused --> Paused: duplicate matching pause
    Paused --> Anomaly: second active pause
    Running --> Anomaly: stale resolution
    Terminal --> Anomaly: later manual fact
```

## Terminal Runs

A `run_terminal` entry fences remaining dispatch work for the run. Rebuilt
projections exclude terminal-run attempts from visible and expired-claim
redelivery views, clear current manual state, and surface later wakeup, claim,
completion, failure, apply, or manual entries for that run as anomalies.
