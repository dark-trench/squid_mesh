defmodule SquidMesh.Runtime.StepExecutor.Outcome.Events do
  @moduledoc """
  Event tuple helpers and post-commit telemetry emission for step outcomes.
  """

  require Logger

  alias SquidMesh.Config
  alias SquidMesh.Observability
  alias SquidMesh.Run
  alias SquidMesh.Runtime.Dispatcher

  @type event ::
          {:step_completed, Run.t(), atom(), pos_integer(), integer()}
          | {:step_failed, Run.t(), atom(), pos_integer(), integer(), map()}
          | {:step_retry_scheduled, Run.t(), atom(), pos_integer(), non_neg_integer()}
          | {:run_transition, Run.t(), Run.status(), Run.status()}
          | {:run_dispatched, Run.t(), map()}
  @type dispatch_result :: {:ok, {:dispatch_events, [event()]}} | {:error, term()}

  @doc false
  @spec emit([event()]) :: :ok
  def emit(events) when is_list(events) do
    Enum.each(events, &emit_one/1)
  end

  @doc false
  @spec completed(Run.t(), atom(), pos_integer(), integer()) :: event()
  def completed(run, step_name, attempt_number, duration) do
    {:step_completed, run, step_name, attempt_number, duration}
  end

  @doc false
  @spec failed(Run.t(), atom(), pos_integer(), integer(), map()) :: event()
  def failed(run, step_name, attempt_number, duration, error) do
    {:step_failed, run, step_name, attempt_number, duration, error}
  end

  @doc false
  @spec retry_scheduled(Run.t(), atom(), pos_integer(), non_neg_integer()) :: event()
  def retry_scheduled(run, step_name, attempt_number, delay_ms) do
    {:step_retry_scheduled, run, step_name, attempt_number, delay_ms}
  end

  @doc false
  @spec transition(Run.t(), Run.status(), Run.status()) :: event()
  def transition(run, from_status, to_status) do
    {:run_transition, run, from_status, to_status}
  end

  @doc false
  @spec dispatch_run(Config.t(), Run.t(), keyword()) :: dispatch_result()
  def dispatch_run(config, run, opts) do
    with {:ok, _jobs, events} <- Dispatcher.dispatch_run_with_events(config, run, opts) do
      {:ok, {:dispatch_events, events}}
    end
  end

  @doc false
  @spec dispatch_steps(Config.t(), Run.t(), [atom()], keyword()) :: dispatch_result()
  def dispatch_steps(config, run, steps, opts) do
    with {:ok, _jobs, events} <- Dispatcher.dispatch_steps_with_events(config, run, steps, opts) do
      {:ok, {:dispatch_events, events}}
    end
  end

  @doc false
  @spec dispatch_compensation(Config.t(), Run.t()) :: dispatch_result()
  def dispatch_compensation(config, run) do
    with {:ok, _job, events} <- Dispatcher.dispatch_compensation_with_events(config, run) do
      {:ok, {:dispatch_events, events}}
    end
  end

  defp emit_one({:step_completed, run, step_name, attempt_number, duration}) do
    Observability.emit_step_completed(run, step_name, attempt_number, duration)
  end

  defp emit_one({:step_failed, run, step_name, attempt_number, duration, error}) do
    Observability.emit_step_failed(run, step_name, attempt_number, duration, error)
  end

  defp emit_one({:step_retry_scheduled, run, step_name, attempt_number, delay_ms}) do
    Logger.warning("workflow step failed; scheduling retry")
    Observability.emit_step_retry_scheduled(run, step_name, attempt_number, delay_ms)
  end

  defp emit_one({:run_transition, run, from_status, to_status}) do
    Observability.emit_run_transition(run, from_status, to_status)
  end

  defp emit_one({:run_dispatched, run, metadata}) do
    Observability.emit_run_dispatched(run, metadata)
  end
end
