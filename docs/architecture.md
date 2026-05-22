# Architecture

Squid Mesh is a workflow automation platform for Elixir applications. It runs
inside a host application's supervision tree and infrastructure.

## Core Components

`SquidMesh.Workflow`

- declarative DSL for triggers, payload, steps, transitions, and retries

`SquidMesh`

- public runtime API for starting, inspecting, listing, cancelling, and replaying runs

`SquidMesh.Runs.Store`

- durable run persistence and run lifecycle transitions

`SquidMesh.Steps.Store`

- durable state for individual workflow steps

`SquidMesh.AttemptStore`

- persisted attempt history per step run

`SquidMesh.Runtime.Dispatcher`

- turns workflow execution intent into calls to the configured host executor

`SquidMesh.Runtime.WorkflowAgent`

- rebuilds per-run workflow coordination state from durable run-thread journal
  entries and checkpoints, including planned runnables, applied results,
  manual pause or approval state, and terminal status

`SquidMesh.Runtime.DispatchAgent`

- rebuilds per-queue dispatch state from durable dispatch-thread journal
  entries and checkpoints, including visible attempts, running leases, retries,
  completed results, failures, and expired claims

`SquidMesh.Runtime.DispatchProtocol`

- defines append-only run, dispatch, and run-index journal entries for the
  Jido-native runtime path; its claim and heartbeat vocabulary is compatible
  with IntentLedger-backed dispatch adapters and refers only to durable
  dispatch fencing metadata, not host-backend worker lifecycle management

`SquidMesh.Runtime.Journal`

- persists dispatch protocol entries and projection checkpoints through
  `Jido.Storage`, preserving Jido thread revision pointers for rebuildable
  runtime projections

`SquidMesh.Runtime.RunIndexProjection`

- rebuilds workflow-scoped run lookup state from run-index journal entries,
  keeping duplicate index facts idempotent and surfacing malformed or
  conflicting index facts as anomalies

`SquidMesh.ReadModel.Inspection`

- rebuilds workflow and dispatch agent projections into a read-only inspection
  snapshot for the Jido-native runtime path, including pending dispatches,
  unapplied results, scheduled attempts, visible attempts, expired claims,
  manual intervention state, terminal state, and projection anomalies

`SquidMesh.ReadModel.Explanation`

- turns a projection-backed inspection snapshot into a deterministic operator
  explanation with reason-specific details, suggested runtime next actions, and
  evidence pointers back to durable journal revisions

`SquidMesh.inspect_run/2` and `SquidMesh.explain_run/2`

- keep the current runtime-table read model as the default public behavior
- accept `read_model: :read_model` with `journal_storage:` when callers
  want to inspect or explain a run from durable Jido journals without
  switching the execution runtime

`SquidMesh.Executor`

- host-implemented behaviour for enqueueing step, compensation, and cron work

`SquidMesh.Runtime.Runner`

- backend-neutral entrypoint that host jobs call when queued work is delivered

`SquidMesh.Runtime.StepExecutor`

- executes one workflow step, merges step output into context, and advances the run

`SquidMesh.Runtime.RetryPolicy`

- resolves step-level retry policy into retry decisions and backoff delays

`SquidMesh.Tools`

- shared boundary for external adapters such as HTTP

## Runtime Responsibilities

Squid Mesh owns:

- workflow structure
- payload validation
- durable run state
- step state and attempt history
- replay and cancellation semantics
- retry policy at the workflow-step layer
- telemetry and structured log metadata

The host executor owns:

- durable job execution
- queueing
- delayed scheduling
- redelivery after worker crashes or restarts

IntentLedger is the preferred future durable executor integration:

- Squid Mesh keeps the workflow and dispatch protocol executor-agnostic.
- An IntentLedger-backed dispatcher can map Squid runnables to Intents and
  translate lifecycle signals back into durable workflow result application.
- Host applications can still provide a custom executor when they need a
  different delivery backend.

Jido owns:

- step behavior execution
- action contracts inside custom step modules
- the storage behaviour used by the Jido-native journal and checkpoint boundary

Postgres owns:

- source-of-truth persistence for runs, steps, and attempts

## Execution Flow

1. A host application starts a run through `SquidMesh.start_run/2`, `start_run/3`, or `start_run/4`.
2. Squid Mesh validates the workflow definition and payload.
3. Squid Mesh persists a pending run in Postgres.
4. The dispatcher asks the configured executor to enqueue the current workflow step.
5. The host job delivers the payload to `SquidMesh.Runtime.Runner.perform/1`.
6. Step output is merged into run context.
7. The runtime decides whether the run completes, advances, retries, fails, or no-ops.
8. If more work is required, the dispatcher asks the executor to enqueue the next step or delayed retry.

## Recovery Boundary

Squid Mesh is intentionally not a replacement for worker coordination in the
host job backend. The Jido-native dispatch protocol records claim, lease, and
result facts for replay and recovery, but concrete worker leasing belongs to
the selected executor backend.

Current guarantees:

- run, step, and attempt history is durable
- queued and scheduled work can survive deploys and restarts when the host executor uses a durable backend
- stale or duplicate deliveries are treated as workflow-level no-ops when possible

Current non-goals:

- a second worker-lifecycle heartbeat or lease manager when a durable executor
  such as IntentLedger already owns that runtime concern
- automatic reclamation of a step that died mid-side-effect
- exactly-once external side effects without idempotent step implementations

## Recommended Reading

- [Positioning](positioning.md)
- [Workflow authoring guide](workflow_authoring.md)
- [Jido runtime architecture](jido_runtime_architecture.md)
- [Durable dispatch protocol](durable_dispatch_protocol.md)
- [Host app integration](host_app_integration.md)
- [Tool adapters](tool_adapters.md)
- [Observability](observability.md)
