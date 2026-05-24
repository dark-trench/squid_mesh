# Squid Mesh Runtime Usage Rules

## Journal Runtime

- Treat the Jido journal runtime as the only execution path.
- Treat journal entries as the durable source of truth.
- Treat checkpoints as rebuild accelerators, not authority.
- Preserve ordered per-thread appends and optimistic conflict detection.
- Rebuild workflow and dispatch projections from persisted facts after
  conflicts, restarts, or checkpoint loss.
- Keep `claim_id` and claim-token fences on heartbeat, completion, and failure.
- Store only claim token hashes in durable entries.
- Apply completed dispatch results to the run thread only after completion is
  durable in the dispatch thread.
- Preserve terminal-run fencing: later claims, completions, manual actions, or
  wakeups for a terminal run must not mutate terminal state.

## Execution

- Execute visible work through `SquidMesh.execute_next/1`.
- Use a stable `owner_id` for workers when possible.
- Keep internal execution controls private; public callers must not pass claim
  tokens or private runner options.
- Retry scheduling must be durable journal intent with a future `visible_at`.
- Built-in `:wait` must create delayed journal intent instead of sleeping in a
  worker.
- Cancellation, replay, pause, approval, rejection, and unblock behavior must
  append durable facts before exposing success.

## Storage

- Keep `SquidMesh.Runtime.Journal.Storage` as the Squid Mesh-owned storage
  boundary.
- Default to Ecto/Postgres-backed Jido storage for documented host setup.
- Keep the boundary database-agnostic, but require production adapters to
  provide ordered appends, conflict detection, deterministic replay, durable
  checkpoint reads, and trusted configuration.
- Never derive `journal_storage` from request input.

## Removed Runtime Surface

- Do not add runtime-table execution paths.
- Do not add stale-step timeout reclaim as public config.
- Do not add table-backed run, step, or attempt stores as runtime authority.
- Do not route normal step execution through `SquidMesh.Executor`.
