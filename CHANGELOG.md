# Changelog

All notable changes to Squid Mesh will be documented in this file.

The format is based on Keep a Changelog and the project follows Semantic
Versioning, including prerelease tags while the runtime remains in early
development.

## [0.1.0-beta.1] - 2026-05-24

### Added
- Opt-in journal executor runtime through `SquidMesh.execute_next/1`, including
  durable `:run_queued` dispatch markers, journal-backed attempt execution,
  dependency progression, retry scheduling, stale-definition fencing through
  `SquidMesh.Workflow.Definition.fingerprint/1`, and `Journal.*` runtime
  modules under `SquidMesh.Runtime.Journal`.
- Durable dispatch-agent claim lifecycle APIs through `DispatchAgent.claim_next/4`,
  `DispatchAgent.heartbeat/6`, `DispatchAgent.complete/7`, and
  `DispatchAgent.fail/7`, including optimistic dispatch-thread fencing,
  claim-token validation, post-append attempt projection returns, retry
  scheduling, and expired lease redelivery support for the Jido-native runtime
  path.
- Durable workflow-agent result application through `WorkflowAgent.apply_result/4`,
  including optimistic run-thread fencing, idempotent duplicate application, and
  rejection of non-completed, wrong-run, terminal-run, and unplanned dispatch
  results before writing.
- Restart recovery for completed dispatch results through
  `WorkflowAgent.apply_pending_results/4`, allowing rebuilt workflow and
  dispatch agents to durably apply results after a lost live wakeup.
- Restart recovery for planned runnables through
  `WorkflowAgent.schedule_pending_dispatches/4` and
  `DispatchAgent.schedule_attempts/5`, allowing rebuilt agents to append missing
  dispatch intents after a crash between workflow planning and dispatch
  scheduling.
- Restart recovery coordination through `AgentRecovery.recover/4`, which
  rebuilds workflow and dispatch agents and drains missing dispatch intents
  before completed dispatch result application.

## [0.1.0-alpha.7] - 2026-05-15

### Added
- Pluggable executor boundary for step execution, delayed scheduling,
  redelivery, and cron activation.
- Native `SquidMesh.Step` modules with raw `Jido.Action` support retained as an
  explicit interop path.
- Durable dispatch protocol documentation and runtime projection invariants for
  dispatch-oriented state.
- Runic workflow planner boundary for workflow graph and mapping facts.
- Jido storage journal boundary, durable rebuild fences, and rebuildable
  runtime agent checkpoints.
- Positioning guide and expanded README usage guidance for embedded workflow
  runtime adoption.

### Changed
- Example workflows now use native Squid Mesh steps instead of raw actions by
  default.
- README and host app setup snippets now reference `0.1.0-beta.1`.
- Dependency join explanations and workflow-centric examples were tightened for
  clearer operator and authoring guidance.

### Fixed
- Planner input/output mappings now align with workflow mappings.
- Dispatch projection validation now preserves explicit nil fields and hardens
  projection invariants.
- Journal replay decoding and agent replay recovery now handle malformed or
  stale persisted data more defensively.

### Notes
- This is the first beta release. Runtime agents, journal-backed projections,
  and dispatch protocol boundaries are now the supported contract and should be
  evaluated with host-app smoke coverage before broader use.

## [0.1.0-alpha.6] - 2026-05-13

### Added
- Saga compensation callbacks with `compensate: SomeAction`, executed in
  reverse completion order after downstream terminal failure with persisted
  recovery history.
- Failure-route recovery markers with `recovery: :compensation | :undo` on
  error transitions, plus `:compensation_routed` and `:undo_routed` audit
  events.
- Local repo transaction groups for custom steps through `transaction: :repo`,
  wrapping the step action in the configured repo transaction.
- Minimal host app examples and smoke coverage for saga checkout, gateway
  credit compensation routing, and local ledger transaction commit/rollback.

### Changed
- Workflow authoring, operations, README, and host app documentation now
  distinguish saga rollback, same-step failure routing, undo, and local repo
  transaction boundaries.
- Runtime failure and compensation paths now preserve more recovery metadata
  for inspection and explainability.

### Fixed
- Hardened compensation outcome handling and terminal target serialization for
  failure recovery routes.
- Nested exception structs in step error details no longer crash error
  normalization.

### Notes
- This remains an alpha release. `transaction: :repo` is a local host repo
  boundary only; it is not a distributed transaction across durable workflow
  progress, Oban dispatch, external systems, or compensation callbacks.

## [0.1.0-alpha.5] - 2026-05-11

### Added
- Step recovery markers with `irreversible: true` and `compensatable: false`
  for workflows that perform side effects which cannot be safely replayed by
  default.
- Persisted recovery policy on step runs, exposed through run inspection,
  declared step state, and run explanations.
- Replay safety checks that block `SquidMesh.replay_run/2` after completed
  irreversible or non-compensatable steps unless the caller passes
  `allow_irreversible: true`.
- Exported formatter rules for Squid Mesh workflow DSL calls, plus host app
  setup guidance for importing them.

### Changed
- Workflow examples and documentation now use the exported DSL formatter style
  without unnecessary parentheses.
- The minimal host app marks its notification step as non-compensatable.

### Fixed
- Recovery marker validation no longer emits a misleading conflict error for
  non-boolean marker values.
- Persisted recovery policy normalization now preserves the invariant that
  irreversible steps are non-compensatable.
- Replay approval requires `allow_irreversible: true` exactly; other truthy
  values no longer bypass the unsafe replay guard.

### Notes
- This remains an alpha release. Existing alpha host apps that already installed
  earlier Squid Mesh migrations should reinstall from the current schema or
  apply an equivalent local migration before writing new step runs.

## [0.1.0-alpha.4] - 2026-05-10

### Added
- Run explanation diagnostics through `SquidMesh.explain_run/2`, including
  current reason, valid next actions, and supporting evidence for host app
  dashboards or CLIs.
- Multiple workflow triggers per workflow, with any mix of manual and cron
  entrypoints and per-trigger payload validation.
- Minimal host app documentation and smoke coverage for a workflow that can be
  started manually or by cron.

### Changed
- `mix squid_mesh.install` now installs one fresh current-schema Squid Mesh
  migration instead of copying the historical split migration set.

### Fixed
- Public run APIs now return structured `:invalid_run_id` errors for malformed
  run IDs.

### Notes
- This release intentionally does not provide a compatibility path for older
  split Squid Mesh migrations. Existing evaluation apps should reinstall from
  the current schema while the project remains in alpha.

## [0.1.0-alpha.3] - 2026-05-07

### Added
- Human-in-the-loop workflow support with paused runs and
  `SquidMesh.unblock_run/2`.
- Approval workflow primitives with `approval_step/2`,
  `SquidMesh.approve_run/3`, and `SquidMesh.reject_run/3`.
- Manual audit history for pause, resume, approval, and rejection actions when
  inspecting runs with `include_history: true`.
- Operations documentation for idempotent side-effect design and stale running
  step recovery.

### Changed
- Paused and approval runs now persist their resume targets and output mapping,
  so existing paused runs keep the same resume behavior across restarts and
  deploys.
- Runtime recovery paths now preserve queued step state more carefully during
  duplicate delivery, cancellation, retry, and dispatch-failure scenarios.
- Stale running step reclaim is opt-in. By default, a duplicate or redelivered
  job skips an already running step instead of starting another attempt after a
  timeout.
- README and guide language now focuses on setup, runtime behavior, and
  operational boundaries.

### Fixed
- Invalid `execution:` configuration now returns structured config errors
  instead of raising during config load.
- Runtime telemetry is emitted after the related durable state commits in
  progression paths that update run or step state.

### Notes
- This remains an alpha release. Steps that call external systems should use
  application-owned idempotency keys or another duplicate-safety strategy.

## [0.1.0-alpha.2] - 2026-05-04

### Added
- Dependency-based workflow steps with `after: [...]`, including durable
  scheduling of ready steps and dependency-aware host app examples.
- Explicit error routing for transition workflows with `transition(..., on:
  :error, to: ...)` after retries are exhausted.
- Explicit step `input: [...]` selection and `output: :key` namespacing for
  clearer data flow between workflow steps.
- Graph-aware inspection with a public `steps` view alongside chronological
  `step_runs` when `inspect_run(..., include_history: true)` is enabled.

### Changed
- Refactored runtime execution into clearer prepare, execute, and apply phases
  without changing the public workflow DSL.
- Hardened dependency-mode concurrency, step claiming, retry progression, and
  terminal-run dispatch behavior across parallel branch execution.
- Expanded host app smoke and integration coverage to exercise dependency
  workflows, mapped step I/O, and nonlinear inspection paths end to end.

### Notes
- This is still an alpha release. The runtime is stronger for evaluation and
  internal integration work, but the production-readiness bar remains unchanged.

## [0.1.0-alpha.1] - 2026-04-28

### Added
- Declarative workflow DSL with manual and cron triggers, payload contracts,
  built-in steps, transitions, and step-level retry/backoff configuration.
- Durable runtime built on Postgres and Oban, including replay, cancellation,
  cron activation, run inspection, and step/attempt history.
- Tool adapter boundary with an HTTP adapter and runtime observability hooks.
- Example host app with smoke, resilience, and bounded soak verification paths.
- Compatibility, operations, and production-readiness documentation.

### Changed
- Clarified the runtime boundary between Squid Mesh, Oban, Jido, and host
  applications across the README and docs.
- Disabled Jido's internal action retries at the Squid Mesh boundary so one
  workflow attempt maps to one persisted step attempt.

### Notes
- This is an alpha release. The runtime is suitable for evaluation, local
  development, and internal integration work, but it is not yet positioned as
  production-ready.
