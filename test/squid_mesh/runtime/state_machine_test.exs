defmodule SquidMesh.Runtime.StateMachineTest do
  use ExUnit.Case, async: true

  alias SquidMesh.Runtime.StateMachine

  describe "states/0" do
    test "enumerates the valid run states" do
      assert StateMachine.states() == [
               :pending,
               :running,
               :retrying,
               :paused,
               :failed,
               :completed,
               :cancelling,
               :cancelled
             ]
    end
  end

  describe "allowed_transitions/1" do
    test "returns the transitions available from a state" do
      assert {:ok, [:running, :failed, :cancelled]} = StateMachine.allowed_transitions(:pending)

      assert {:ok, [:retrying, :paused, :failed, :completed, :cancelling]} =
               StateMachine.allowed_transitions(:running)

      assert {:ok, [:running, :failed, :completed, :cancelled]} =
               StateMachine.allowed_transitions(:paused)

      assert {:ok, [:cancelled, :failed]} = StateMachine.allowed_transitions(:cancelling)
    end

    test "rejects unknown states" do
      assert {:error, {:unknown_state, :bogus}} = StateMachine.allowed_transitions(:bogus)
    end
  end

  describe "transition/2" do
    test "accepts valid transitions needed by the executor lifecycle" do
      assert {:ok, :running} = StateMachine.transition(:pending, :running)
      assert {:ok, :retrying} = StateMachine.transition(:running, :retrying)
      assert {:ok, :paused} = StateMachine.transition(:running, :paused)
      assert {:ok, :running} = StateMachine.transition(:retrying, :running)
      assert {:ok, :running} = StateMachine.transition(:paused, :running)
      assert {:ok, :completed} = StateMachine.transition(:running, :completed)
      assert {:ok, :completed} = StateMachine.transition(:paused, :completed)
      assert {:ok, :cancelling} = StateMachine.transition(:running, :cancelling)
      assert {:ok, :cancelled} = StateMachine.transition(:cancelling, :cancelled)
      assert {:ok, :cancelled} = StateMachine.transition(:paused, :cancelled)
    end

    test "rejects invalid transitions" do
      assert {:error, {:invalid_transition, :pending, :completed}} =
               StateMachine.transition(:pending, :completed)

      assert {:error, {:invalid_transition, :completed, :running}} =
               StateMachine.transition(:completed, :running)

      assert {:error, {:invalid_transition, :cancelled, :retrying}} =
               StateMachine.transition(:cancelled, :retrying)
    end

    test "rejects unknown origin and destination states" do
      assert {:error, {:unknown_state, :bogus}} = StateMachine.transition(:bogus, :running)
      assert {:error, {:unknown_state, :bogus}} = StateMachine.transition(:running, :bogus)
    end
  end

  describe "terminal?/1" do
    test "identifies terminal states" do
      assert StateMachine.terminal?(:failed)
      assert StateMachine.terminal?(:completed)
      assert StateMachine.terminal?(:cancelled)

      refute StateMachine.terminal?(:pending)
      refute StateMachine.terminal?(:running)
      refute StateMachine.terminal?(:retrying)
      refute StateMachine.terminal?(:paused)
      refute StateMachine.terminal?(:cancelling)
    end
  end

  describe "can_transition?/2" do
    test "returns a boolean view over transition validity" do
      assert StateMachine.can_transition?(:pending, :running)
      assert StateMachine.can_transition?(:running, :failed)
      assert StateMachine.can_transition?(:paused, :running)

      refute StateMachine.can_transition?(:pending, :completed)
      refute StateMachine.can_transition?(:paused, :retrying)
    end
  end

  describe "schedule_next_step?/1" do
    test "allows scheduling only for active execution states" do
      assert StateMachine.schedule_next_step?(:pending)
      assert StateMachine.schedule_next_step?(:running)
      assert StateMachine.schedule_next_step?(:retrying)

      refute StateMachine.schedule_next_step?(:paused)
      refute StateMachine.schedule_next_step?(:cancelling)
      refute StateMachine.schedule_next_step?(:cancelled)
      refute StateMachine.schedule_next_step?(:failed)
      refute StateMachine.schedule_next_step?(:completed)
    end
  end
end
