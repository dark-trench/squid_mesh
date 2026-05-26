# Squid Mesh Tooling Usage Rules

## Dashboard And Visual Editor Surfaces

- Use `SquidMesh.list_runs/2` for global and workflow-filtered run indexes.
- Use `SquidMesh.inspect_run/2` for factual run details.
- Use `SquidMesh.inspect_run_graph/2` for graph-oriented dashboard and visual
  editor views.
- Use `SquidMesh.explain_run/2` for operator-facing diagnosis and next actions.
- Keep list responses redacted by default; fetch detailed history only when the
  caller asks for it.
- Use `definition_version` from list, inspection, graph, and explanation
  surfaces as an operator label only; the definition fingerprint remains the
  compatibility guard.

## Workflow Specs

- Use `SquidMesh.Workflow.to_spec/1` for normalized workflow definitions.
- Use `SquidMesh.Workflow.validate_spec/1` before trusting a spec in tooling.
- Treat specs as data for editors, diagrams, and validation; do not use them as
  a module ownership allowlist.
- Preserve stable ids for workflow, trigger, step, transition, and condition
  data so visual editors can round-trip safely.

## SquidSonar

- SquidSonar should list existing workflows and runs through public Squid Mesh
  APIs rather than reading storage adapter internals.
- SquidSonar should fetch specific workflow internals by module/spec and
  specific run internals by run id.
