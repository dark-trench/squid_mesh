# Compatibility Matrix

This matrix defines the currently supported baseline for Squid Mesh.

## Supported Baseline

| Component | Supported baseline |
| --- | --- |
| Elixir | `1.19.5-otp-28` |
| Erlang/OTP | `28.4.1` |
| Postgres | `15+` |
| Jido | `2.0+` |

## What Supported Means

For the current release line, "supported" means:

- the combination is exercised in CI or repeatable local verification
- the documentation and example harnesses target that baseline
- bug reports on that baseline are in scope for active support

## Host App Expectations

Supported host apps are expected to provide:

- an Ecto `Repo`
- Postgres for durable state
- a supervised worker that calls `SquidMesh.execute_next/1`
- a scheduler that can deliver cron payloads to `SquidMesh.Runtime.Runner.perform/2`, if the app uses cron triggers
- step modules that conform to the current Squid Mesh action contract

## Version Evaluation Policy

Before a new version is called supported, the team should:

1. Run the root test suite.
2. Run the example host app smoke path.
3. Run the restart resilience and soak/load verifications in the example app, including paused-run unblock after restart.
4. Review docs and configuration snippets for version-specific drift.

Until that work is done, newer versions may still work, but they should be
treated as unverified rather than supported.
