defmodule SquidMesh.Runs.GraphInspectionTest do
  use ExUnit.Case, async: true

  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.Runs.GraphInspection

  @run_id "run_123"
  @child_run %{
    child_run_id: "child_run_123",
    child_workflow: "ChildWorkflow",
    child_trigger: "manual",
    child_key: "digest_subscription_1",
    origin: %{runnable_key: "run_123:fanout:1", step: "fanout", attempt: 1},
    metadata: %{subscription_id: "sub_123"}
  }

  test "exposes child runs as graph metadata instead of inline nodes" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "MissingWorkflow",
      queue: "default",
      status: :running,
      reason: :run_started,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      child_runs: [@child_run]
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)

    assert graph.child_runs == [@child_run]
    assert graph.nodes == []

    assert %{child_runs: [@child_run], nodes: []} = GraphInspection.to_map(graph)
  end

  test "exposes stable action identity from planned runnable metadata" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "RuntimeAuthoredWorkflow",
      queue: "default",
      status: :running,
      reason: :planned_dispatch_pending_schedule,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      planned_runnables: [
        %{
          step: :load_invoice,
          metadata: %{action: "billing.load_invoice"}
        }
      ]
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)

    assert [%{id: "load_invoice", action: "billing.load_invoice"}] =
             Enum.map(graph.nodes, &Map.take(&1, [:id, :action]))

    assert %{nodes: [%{id: "load_invoice", action: "billing.load_invoice"}]} =
             GraphInspection.to_map(graph)
  end
end
