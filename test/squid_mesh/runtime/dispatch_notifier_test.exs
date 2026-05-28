defmodule SquidMesh.Runtime.DispatchNotifierTest do
  use ExUnit.Case, async: true

  alias SquidMesh.Runtime.DispatchNotifier

  @attempt %{
    run_id: "run_123",
    runnable_key: "run_123:charge_card:1",
    queue: "default",
    visible_at: ~U[2026-05-15 00:00:10Z]
  }

  test "returns a structured error when notifier does not implement the callback" do
    assert {:error, {:invalid_notifier, String}} =
             DispatchNotifier.notify_attempt_scheduled(String, @attempt, [])
  end

  test "noop notifier accepts scheduled attempt wakeups" do
    assert :ok =
             DispatchNotifier.notify_attempt_scheduled(
               DispatchNotifier.Noop,
               @attempt,
               []
             )
  end
end
