defmodule SquidMesh.Executor do
  @moduledoc """
  Behaviour implemented by host applications to deliver Squid Mesh work.

  Squid Mesh owns workflow state, retry decisions, and progression. The host
  executor owns how runnable work is queued and later delivered back to the
  runtime entrypoints in `SquidMesh.Runtime.Runner`.

  A typical executor converts the run intent into `SquidMesh.Executor.Payload`
  data, stores that payload in the host app's job backend, and returns metadata
  about the queued job.

      def enqueue_step(_config, run, step, opts) do
        job = %{
          payload: SquidMesh.Executor.Payload.step(run, step),
          queue: queue(),
          schedule_in: opts[:schedule_in]
        }

        with {:ok, queued_job} <- MyApp.JobQueue.enqueue(job) do
          {:ok, %{job_id: queued_job.id, queue: queued_job.queue}}
        end
      end

  The queued job should call `SquidMesh.Runtime.Runner.perform/2` with the
  stored payload. The job backend, queue name, retry settings, and scheduler
  stay in the host application.
  """

  alias SquidMesh.Config
  alias SquidMesh.Run

  @type metadata :: map()
  @type enqueue_error :: term()
  @type schedule_window :: %{
          optional(:start_at) => String.t(),
          optional(:end_at) => String.t(),
          optional(String.t()) => String.t()
        }
  @type enqueue_opts :: [schedule_in: pos_integer()]
  @type cron_enqueue_opts :: [
          {:schedule_in, pos_integer()}
          | {:signal_id, String.t()}
          | {:intended_window, schedule_window()}
        ]

  @doc """
  Enqueues one workflow step for execution.

  `opts[:schedule_in]` is present for delayed retry and wait continuation jobs.
  Return metadata such as `:job_id`, `:queue`, `:worker`, and `:schedule_in` for
  dispatch telemetry.
  """
  @callback enqueue_step(Config.t(), Run.t(), atom(), enqueue_opts()) ::
              {:ok, metadata()} | {:error, enqueue_error()}

  @doc """
  Enqueues multiple runnable dependency-root steps for the same run.

  Implementations should enqueue the full batch atomically when their job
  backend supports it. If enqueueing fails, return `{:error, reason}` so Squid
  Mesh can roll back pending step rows.
  """
  @callback enqueue_steps(Config.t(), Run.t(), [atom()], enqueue_opts()) ::
              {:ok, [metadata()]} | {:error, enqueue_error()}

  @doc """
  Enqueues compensation for a failed run with completed reversible steps.
  """
  @callback enqueue_compensation(Config.t(), Run.t(), enqueue_opts()) ::
              {:ok, metadata()} | {:error, enqueue_error()}

  @doc """
  Enqueues or schedules a cron trigger activation.

  Host schedulers can call this callback when a declared cron trigger fires, or
  can enqueue `SquidMesh.Executor.Payload.cron/3` directly and deliver it to
  `SquidMesh.Runtime.Runner.perform/2`.

  When the scheduler knows the logical schedule window, pass `:signal_id` and
  `:intended_window` through to `SquidMesh.Executor.Payload.cron/3`. Squid Mesh
  persists those values as run context before workflow processing starts, so
  delayed workers do not need to infer the intended window from wall-clock time.

  Cron triggers that opt into idempotency use the scheduler `:signal_id`, or a
  deterministic id derived from a complete `:intended_window`, as the duplicate
  start key. If the host omits both, the runtime rejects the start because it
  cannot safely distinguish a new activation from a redelivery.
  """
  @callback enqueue_cron(Config.t(), module(), atom(), cron_enqueue_opts()) ::
              {:ok, metadata()} | {:error, enqueue_error()}

  @required_callbacks [
    enqueue_step: 4,
    enqueue_steps: 4,
    enqueue_compensation: 3,
    enqueue_cron: 4
  ]

  @doc false
  @spec required_callbacks() :: keyword(pos_integer())
  def required_callbacks, do: @required_callbacks
end
