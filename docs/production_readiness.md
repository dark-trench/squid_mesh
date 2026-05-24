# Production Readiness

Squid Mesh is still marked as early development.

That warning remains until the checklist below is satisfied and reviewed as a
whole. The goal is to keep the public contract aligned with the verification
that has actually been run.

## Current Status

What exists today:

- durable workflow runtime on top of Postgres-backed Jido journals and
  host-supervised `execute_next/1` workers
- replay, cancellation, retries, cron activation, and inspection
- example host app harness with smoke, cancellation, restart resilience, and soak/load entrypoints
- paused-run resume semantics now verified across restart boundaries in the example host app
- explicit recovery and operational boundaries in the docs

Why the warning still remains:

- support is still defined from a narrow verified baseline
- soak/load validation is bounded verification, not long-term operational evidence
- broader dogfooding and real production adoption history still matter

## Readiness Checklist

Before removing the warning, the project should have:

- a published compatibility matrix
- a production operations guide
- restart and deploy resilience verification
- soak/load validation on the journal-backed runtime
- no known unresolved correctness bug in the core runtime
- at least one round of real host-app dogfooding under normal deploy workflows

## Example Verification Entry Points

The example host app provides the repeatable checks:

```sh
cd examples/minimal_host_app
MIX_ENV=test mix example.smoke
MIX_ENV=test mix example.resilience
MIX_ENV=test mix example.soak
```

These checks are meant to answer different questions:

- `example.smoke`: does the basic embedded workflow path work?
- `example.resilience`: do queued, delayed, retrying, and paused-then-resumed runs survive worker and scheduler restart boundaries?
- `example.soak`: does the runtime remain stable under a bounded mix of success, retry, replay, and cancellation traffic?

## Decision Rule

The warning should be removed only when:

1. the checklist above is complete,
2. the verification paths are green on the supported baseline,
3. maintainers are ready to support the documented contract in production host apps.

Until then, Squid Mesh should continue to describe itself as suitable for:

- evaluation
- local development
- internal integration work
