defmodule BedrockMinimalHostApp.SquidMeshExecutor do
  @moduledoc """
  Bedrock-backed Squid Mesh executor owned by the host app.
  """

  @behaviour SquidMesh.Executor

  alias SquidMesh.Executor.Payload

  @impl true
  def enqueue_step(_config, run, step, opts) do
    run
    |> Payload.step(step)
    |> enqueue(opts)
  end

  @impl true
  def enqueue_steps(_config, run, steps, opts) do
    # This exercises Bedrock's queue boundary only. The current Squid Mesh
    # executor contract does not give this adapter an atomic batch enqueue.
    results =
      Enum.map(steps, fn step ->
        run
        |> Payload.step(step)
        |> enqueue(opts)
      end)

    with :ok <- assert_all_enqueued(results) do
      {:ok, Enum.map(results, fn {:ok, metadata} -> metadata end)}
    end
  end

  @impl true
  def enqueue_compensation(_config, run, opts) do
    run
    |> Payload.compensation()
    |> enqueue(opts)
  end

  @impl true
  def enqueue_cron(_config, workflow, trigger, opts) do
    workflow
    |> Payload.cron(trigger, Keyword.take(opts, [:signal_id, :intended_window]))
    |> enqueue(opts)
  end

  def queue do
    Keyword.get(executor_config(), :queue_id, "default")
  end

  defp enqueue(payload, opts) do
    queue_id = queue()
    topic = topic()

    # Bedrock owns job visibility, retries, and leases in this spike. Squid Mesh
    # still owns workflow facts through its configured Ecto repo.
    case BedrockMinimalHostApp.JobQueue.enqueue(queue_id, topic, payload, job_opts(opts)) do
      {:ok, item} -> {:ok, metadata(item, queue_id, topic)}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_enqueue_result, other}}
    end
  end

  defp assert_all_enqueued(results) do
    case Enum.find(results, &match?({:error, _reason}, &1)) do
      nil -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp executor_config do
    Application.get_env(:bedrock_minimal_host_app, __MODULE__, [])
  end

  defp job_opts(opts) do
    maybe_put_delay([], Keyword.get(opts, :schedule_in))
  end

  defp maybe_put_delay(opts, schedule_in)
       when is_integer(schedule_in) and schedule_in > 0 do
    Keyword.put(opts, :in, schedule_in)
  end

  defp maybe_put_delay(opts, _schedule_in), do: opts

  defp topic do
    Keyword.get(executor_config(), :topic, "squid_mesh:payload")
  end

  defp metadata(item, queue_id, topic) do
    %{
      item_id: item.id,
      executor: __MODULE__,
      queue: queue_id,
      topic: topic,
      scheduled_at: item.vesting_time
    }
  end
end
