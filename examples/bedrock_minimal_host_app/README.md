# Bedrock Minimal Host App

Reference host-app harness for testing Squid Mesh with Bedrock Job Queue as the
delivery backend.

This example keeps two storage boundaries visible:

- Squid Mesh workflow state is stored through `BedrockMinimalHostApp.Repo`.
- Bedrock Job Queue owns queued jobs, delayed delivery, leases, retries, and
  queue metadata through the embedded Bedrock cluster.

## Setup

Start a local Postgres instance and point `DATABASE_URL` at it. The default is:

```sh
ecto://postgres:postgres@localhost/bedrock_minimal_host_app_dev
```

Then set up the example app:

```sh
mix setup
```

This will:

- create the example app database
- install Squid Mesh migrations into the example app with `mix squid_mesh.install`
- run the example app and Squid Mesh migrations through `mix ecto.migrate`

Bedrock runs embedded for the spike. In local and test mode it uses configured
filesystem paths for cluster state; production hosts should configure durable
Bedrock storage or a real cluster topology.

## Stress Test

Run the Bedrock job queue stress coverage:

```sh
MIX_ENV=test mix test test/bedrock_job_queue_stress_test.exs test/bedrock_minimal_host_app/squid_mesh_lease_adapter_test.exs
```

The stress test covers:

- topic routing and tenant queue isolation
- priority ordering
- delayed job visibility
- leasing and lease extension
- retry requeue and dead-letter behavior
- Squid Mesh cron payloads being mapped into Bedrock jobs
- the `SquidMesh.Executor.Leases` contract through a Bedrock-backed example
  adapter

The example intentionally does not include another job backend. That keeps the
adapter boundary clear while the spike evaluates Bedrock as the host-owned
delivery and leasing layer for Jido-native Squid Mesh execution.
