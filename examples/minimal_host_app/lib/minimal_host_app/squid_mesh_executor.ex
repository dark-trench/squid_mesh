defmodule MinimalHostApp.SquidMeshExecutor do
  @moduledoc """
  Oban-backed Squid Mesh executor owned by the host app.
  """

  @behaviour SquidMesh.Executor

  alias MinimalHostApp.Workers.SquidMeshWorker
  alias SquidMesh.Executor.Payload

  @impl true
  def enqueue_step(_config, run, step, opts) do
    changeset =
      run
      |> Payload.step(step)
      |> SquidMeshWorker.new(job_opts(opts))

    oban_name()
    |> Oban.insert(changeset)
    |> normalize_insert_result()
  end

  @impl true
  def enqueue_steps(_config, run, steps, opts) do
    changesets =
      Enum.map(steps, fn step ->
        run
        |> Payload.step(step)
        |> SquidMeshWorker.new(job_opts(opts))
      end)

    oban_name()
    |> Oban.insert_all(changesets)
    |> normalize_insert_all_result()
  end

  @impl true
  def enqueue_compensation(_config, run, opts) do
    changeset =
      run
      |> Payload.compensation()
      |> SquidMeshWorker.new(job_opts(opts))

    oban_name()
    |> Oban.insert(changeset)
    |> normalize_insert_result()
  end

  @impl true
  def enqueue_cron(_config, workflow, trigger, opts) do
    changeset =
      workflow
      |> Payload.cron(trigger, Keyword.take(opts, [:signal_id, :intended_window]))
      |> SquidMeshWorker.new(job_opts(opts))

    oban_name()
    |> Oban.insert(changeset)
    |> normalize_insert_result()
  end

  def queue do
    executor_config()
    |> Keyword.get(:queue, :squid_mesh)
  end

  defp job_opts(opts) do
    [queue: queue()]
    |> maybe_put_schedule_in(Keyword.get(opts, :schedule_in))
  end

  defp oban_name do
    executor_config()
    |> Keyword.get(:oban_name, Oban)
  end

  defp executor_config do
    Application.get_env(:minimal_host_app, __MODULE__, [])
  end

  defp maybe_put_schedule_in(opts, schedule_in)
       when is_integer(schedule_in) and schedule_in > 0 do
    Keyword.put(opts, :schedule_in, schedule_in)
  end

  defp maybe_put_schedule_in(opts, _schedule_in), do: opts

  defp normalize_insert_result({:ok, job}), do: {:ok, metadata(job)}
  defp normalize_insert_result({:error, reason}), do: {:error, reason}
  defp normalize_insert_result(other), do: {:error, {:unexpected_insert_result, other}}

  defp normalize_insert_all_result({:ok, jobs}) when is_list(jobs),
    do: {:ok, Enum.map(jobs, &metadata/1)}

  defp normalize_insert_all_result(jobs) when is_list(jobs),
    do: {:ok, Enum.map(jobs, &metadata/1)}

  defp normalize_insert_all_result({:error, reason}), do: {:error, reason}
  defp normalize_insert_all_result(other), do: {:error, {:unexpected_insert_all_result, other}}

  defp metadata(%Oban.Job{} = job) do
    %{
      job_id: job.id,
      executor: __MODULE__,
      queue: queue(),
      worker: job.worker,
      scheduled_at: job.scheduled_at
    }
  end
end
