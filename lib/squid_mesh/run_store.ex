defmodule SquidMesh.RunStore do
  @moduledoc """
  Compatibility facade for the run store.

  New internal code should use `SquidMesh.Runs.Store`.
  """

  alias SquidMesh.Runs.Store

  @type list_filter :: Store.list_filter()
  @type list_filters :: Store.list_filters()
  @type create_error :: Store.create_error()
  @type get_error :: Store.get_error()
  @type transition_attrs :: Store.transition_attrs()
  @type transition_error :: Store.transition_error()
  @type replay_error :: Store.replay_error()
  @type create_option :: Store.create_option()
  @type replay_option :: Store.replay_option()
  @type update_error :: Store.update_error()
  @type get_option :: Store.get_option()
  @type dispatch_fun :: Store.dispatch_fun()
  @type attrs_fun :: Store.attrs_fun()
  @type failure_attrs_fun :: Store.failure_attrs_fun()
  @type run_transition_event :: Store.run_transition_event()
  @type progress_event :: Store.progress_event()
  @type progress_operation :: Store.progress_operation()
  @type progress_result :: Store.progress_result()
  @type pause_result :: Store.pause_result()

  defdelegate create_run(repo, workflow, payload), to: Store
  defdelegate create_run(repo, workflow, trigger_name, payload), to: Store
  defdelegate replay_run(repo, run_id), to: Store
  defdelegate replay_run(repo, run_id, opts), to: Store
  defdelegate create_and_dispatch_run(repo, workflow, payload, dispatch_fun), to: Store
  defdelegate create_and_dispatch_run(repo, workflow, arg3, arg4, arg5), to: Store

  defdelegate create_and_dispatch_run(repo, workflow, trigger_name, payload, dispatch_fun, opts),
    to: Store

  defdelegate replay_and_dispatch_run(repo, run_id, dispatch_fun), to: Store
  defdelegate replay_and_dispatch_run(repo, run_id, dispatch_fun, opts), to: Store
  defdelegate get_run(repo, run_id), to: Store
  defdelegate get_run(repo, run_id, opts), to: Store
  defdelegate get_run_for_update(repo, run_id), to: Store
  defdelegate get_run_by_schedule_idempotency(repo, identity), to: Store
  defdelegate list_runs(repo), to: Store
  defdelegate list_runs(repo, filters), to: Store
  defdelegate transition_run(repo, run_id, to_status), to: Store
  defdelegate transition_run(repo, run_id, to_status, attrs), to: Store
  defdelegate transition_run_silent(repo, run_id, to_status), to: Store
  defdelegate transition_run_silent(repo, run_id, to_status, attrs), to: Store
  defdelegate transition_and_dispatch_run(repo, run_id, to_status, attrs, dispatch_fun), to: Store
  defdelegate cancel_run(repo, run_id), to: Store
  defdelegate update_run(repo, run_id, attrs), to: Store
  defdelegate update_run_with(repo, run_id, attrs_fun), to: Store
  defdelegate progress_run_with(repo, run_id, attrs_fun, operation), to: Store
  defdelegate progress_run_with_events(repo, run_id, attrs_fun, operation), to: Store
  defdelegate update_and_dispatch_run(repo, run_id, attrs, dispatch_fun), to: Store
  defdelegate update_and_dispatch_run_with(repo, run_id, attrs_fun, dispatch_fun), to: Store
  defdelegate pause_run(repo, run_id, step_run_id, attempt_id, attrs), to: Store
  defdelegate transition_run_with(repo, run_id, to_status, attrs_fun), to: Store
  defdelegate schedule_next_step?(run_or_status), to: Store
  defdelegate pause_cancellation_error(), to: Store
end
