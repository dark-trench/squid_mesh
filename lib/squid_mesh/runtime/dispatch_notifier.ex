defmodule SquidMesh.Runtime.DispatchNotifier do
  @moduledoc """
  Boundary for live dispatch wakeups emitted after durable scheduling.

  Wakeups are hints only. Durable dispatch entries remain the source of truth,
  so a notifier failure must not roll back or hide a scheduled attempt.
  """

  @type attempt :: %{
          required(:run_id) => String.t(),
          required(:runnable_key) => String.t(),
          required(:queue) => String.t(),
          required(:visible_at) => DateTime.t()
        }

  @callback notify_attempt_scheduled(attempt(), keyword()) :: :ok | {:error, term()}

  @doc """
  Emits a live wakeup hint for a scheduled attempt through the configured notifier.
  """
  @spec notify_attempt_scheduled(module(), attempt(), keyword()) :: :ok | {:error, term()}
  def notify_attempt_scheduled(notifier, attempt, opts)
      when is_atom(notifier) and is_map(attempt) and is_list(opts) do
    if function_exported?(notifier, :notify_attempt_scheduled, 2) do
      notifier.notify_attempt_scheduled(attempt, opts)
    else
      {:error, {:invalid_notifier, notifier}}
    end
  end
end

defmodule SquidMesh.Runtime.DispatchNotifier.Noop do
  @moduledoc false

  @behaviour SquidMesh.Runtime.DispatchNotifier

  @impl SquidMesh.Runtime.DispatchNotifier
  def notify_attempt_scheduled(_attempt, _opts), do: :ok
end
