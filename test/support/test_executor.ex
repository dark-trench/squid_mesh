defmodule SquidMesh.Test.Executor do
  @moduledoc false

  @behaviour SquidMesh.Executor

  use Agent

  alias SquidMesh.Executor.Payload
  alias SquidMesh.Runtime.Runner
  alias SquidMesh.Test.Job

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{jobs: [], fail?: false} end, name: __MODULE__)
  end

  def reset! do
    Agent.update(__MODULE__, fn _state -> %{jobs: [], fail?: false} end)
  end

  def fail_next! do
    Agent.update(__MODULE__, &Map.put(&1, :fail?, true))
  end

  def jobs do
    Agent.get(__MODULE__, & &1.jobs)
  end

  def available_count(run_id, step \\ nil) do
    jobs()
    |> Enum.count(&(available?(&1) and matches_run_and_step?(&1, run_id, step)))
  end

  def scheduled_job(run_id, step \\ nil) do
    jobs()
    |> Enum.filter(&(scheduled?(&1) and matches_run_and_step?(&1, run_id, step)))
    |> Enum.sort_by(& &1.inserted_at, {:desc, DateTime})
    |> List.first()
  end

  def compensation_job(run_id) do
    jobs()
    |> Enum.filter(&(&1.args["run_id"] == run_id and &1.args["kind"] == "compensation"))
    |> Enum.sort_by(& &1.inserted_at, {:desc, DateTime})
    |> List.first()
  end

  def drain do
    do_drain(0)
  end

  def drain_one do
    case pop_job() do
      nil ->
        %{success: 0, failure: 0}

      job ->
        :ok = perform(job)
        %{success: 1, failure: 0}
    end
  end

  @impl true
  def enqueue_step(_config, run, step, opts) do
    enqueue(%Job{
      id: Ecto.UUID.generate(),
      worker: "SquidMesh.Test.StepWorker",
      queue: "squid_mesh",
      args: Payload.step(run, step),
      inserted_at: DateTime.utc_now(),
      scheduled_at: scheduled_at(opts),
      meta: %{kind: :step, opts: opts}
    })
  end

  @impl true
  def enqueue_steps(config, run, steps, opts) do
    steps
    |> Enum.reduce_while({:ok, []}, fn step, {:ok, metadata} ->
      case enqueue_step(config, run, step, opts) do
        {:ok, job_metadata} -> {:cont, {:ok, [job_metadata | metadata]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, metadata} -> {:ok, Enum.reverse(metadata)}
      {:error, _reason} = error -> error
    end
  end

  @impl true
  def enqueue_compensation(_config, run, opts) do
    enqueue(%Job{
      id: Ecto.UUID.generate(),
      worker: "SquidMesh.Test.StepWorker",
      queue: "squid_mesh",
      args: Payload.compensation(run),
      inserted_at: DateTime.utc_now(),
      scheduled_at: scheduled_at(opts),
      meta: %{kind: :compensation, opts: opts}
    })
  end

  @impl true
  def enqueue_cron(_config, workflow, trigger, opts) do
    enqueue(%Job{
      id: Ecto.UUID.generate(),
      worker: "SquidMesh.Test.CronTriggerWorker",
      queue: "squid_mesh",
      args: Payload.cron(workflow, trigger),
      inserted_at: DateTime.utc_now(),
      scheduled_at: scheduled_at(opts),
      meta: %{kind: :cron, opts: opts}
    })
  end

  defp enqueue(%Job{} = job) do
    Agent.get_and_update(__MODULE__, fn %{jobs: jobs, fail?: fail?} = state ->
      if fail? do
        {{:error, :executor_unavailable}, %{state | fail?: false}}
      else
        metadata = metadata(job)
        {{:ok, metadata}, %{state | jobs: jobs ++ [job]}}
      end
    end)
  end

  defp do_drain(success_count) do
    case pop_job() do
      nil ->
        %{success: success_count, failure: 0}

      job ->
        :ok = perform(job)
        do_drain(success_count + 1)
    end
  end

  defp pop_job do
    Agent.get_and_update(__MODULE__, fn %{jobs: jobs} = state ->
      pop_available_job(jobs, state)
    end)
  end

  defp pop_available_job([], state), do: {nil, state}

  defp pop_available_job(jobs, state) do
    case Enum.split_while(jobs, &(not available?(&1))) do
      {_scheduled_jobs, []} ->
        {nil, state}

      {scheduled_jobs, [job | remaining]} ->
        {job, %{state | jobs: scheduled_jobs ++ remaining}}
    end
  end

  defp perform(%Job{args: args}) do
    Runner.perform(args)
  end

  defp metadata(%Job{} = job) do
    %{
      job_id: job.id,
      executor: __MODULE__,
      queue: :squid_mesh,
      worker: job.worker,
      schedule_in: get_in(job.meta, [:opts, :schedule_in])
    }
  end

  defp available?(%Job{scheduled_at: nil}), do: true
  defp available?(%Job{}), do: false

  defp scheduled?(%Job{scheduled_at: %DateTime{}}), do: true
  defp scheduled?(%Job{}), do: false

  defp matches_run_and_step?(%Job{} = job, run_id, nil), do: job.args["run_id"] == run_id

  defp matches_run_and_step?(%Job{} = job, run_id, step) do
    job.args["run_id"] == run_id and job.args["step"] == step
  end

  defp scheduled_at(opts) do
    case Keyword.get(opts, :schedule_in) do
      seconds when is_integer(seconds) and seconds > 0 ->
        DateTime.add(DateTime.utc_now(), seconds, :second)

      _other ->
        nil
    end
  end
end
