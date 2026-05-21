defmodule SquidMesh.StepRunStore do
  @moduledoc """
  Compatibility facade for the step run store.

  New internal code should use `SquidMesh.Steps.Store`.
  """

  alias SquidMesh.Steps.Store

  @type step_identifier :: Store.step_identifier()
  @type step_input :: Store.step_input()
  @type step_output :: Store.step_output()
  @type step_error :: Store.step_error()
  @type recovery_policy :: Store.recovery_policy()
  @type failure_recovery :: Store.failure_recovery()
  @type pause_target :: Store.pause_target()
  @type approval_targets :: Store.approval_targets()
  @type manual_event :: Store.manual_event()
  @type step_status :: Store.step_status()
  @type stale_error :: Store.stale_error()
  @type recovery_attrs :: Store.recovery_attrs()
  @type begin_result :: Store.begin_result()
  @type schedule_result :: Store.schedule_result()
  @type step_schedule_input :: Store.step_schedule_input()

  defdelegate begin_step(repo, run_id, step, input), to: Store
  defdelegate begin_step(repo, run_id, step, input, recovery), to: Store
  defdelegate schedule_step(repo, run_id, step, input), to: Store
  defdelegate schedule_step(repo, run_id, step, input, recovery), to: Store
  defdelegate schedule_steps(repo, run_id, step_inputs), to: Store
  defdelegate delete_pending_steps(repo, run_id, steps), to: Store
  defdelegate complete_step(repo, step_run_id, output), to: Store
  defdelegate complete_manual_step(repo, step_run_id, output, manual), to: Store
  defdelegate fail_step(repo, step_run_id, error), to: Store
  defdelegate record_failure_recovery(repo, step_run_id, failure), to: Store
  defdelegate persist_pause_resume(repo, step_run_id, output, target), to: Store
  defdelegate persist_approval_resume(repo, step_run_id, targets, output_key), to: Store
  defdelegate get_step_run(repo, run_id, step), to: Store
  defdelegate completed_steps(repo, run_id), to: Store
  defdelegate completed_outputs(repo, run_id), to: Store
  defdelegate completed_step_runs_for_compensation(repo, run_id), to: Store
  defdelegate update_recovery(repo, step_run_id, recovery), to: Store
  defdelegate step_statuses(repo, run_id), to: Store
end
