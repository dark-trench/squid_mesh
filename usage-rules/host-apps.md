# Squid Mesh Host App Usage Rules

## Configuration

- Configure Squid Mesh with the host repo and queue:

  ```elixir
  config :squid_mesh,
    repo: MyApp.Repo,
    queue: "default"
  ```

- Do not configure `:executor` for step execution.
- Do not configure `:stale_step_timeout`.
- Use explicit `journal_storage` only when replacing the default inferred Ecto
  storage boundary.

## Worker Loop

- Start one or more supervised workers that call `SquidMesh.execute_next/1`.
- Back off briefly when `execute_next/1` returns `{:ok, :none}`.
- Add metrics, capacity limits, and shutdown behavior around the public call
  rather than inside workflow modules.
- Keep workers generic. They should not encode workflow-specific business
  decisions.

## Cron

- Declare cron triggers in workflow modules.
- Keep recurring scheduling in the host app.
- Deliver cron activations with `SquidMesh.Executor.Payload.cron/3` and
  `SquidMesh.Runtime.Runner.perform/2`.
- Include `signal_id` or a complete `intended_window` for idempotent cron
  triggers.
- Do not deliver step or compensation payloads through `Runner.perform/2`.

## Bedrock

- Use Bedrock when the host needs durable backend delivery, delayed visibility,
  lease ownership, heartbeats, retry requeue, dead-letter behavior, or
  distributed worker recovery.
- Keep Bedrock code in adapter modules.
- Do not let workflow modules depend on Bedrock APIs.
- Use `examples/bedrock_minimal_host_app` as the reference integration shape.
