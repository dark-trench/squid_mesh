defmodule SquidMesh.Config do
  @moduledoc """
  Loads and validates host application configuration for Squid Mesh.

  This contract is intentionally small so application teams only configure the
  runtime boundary once, while workflow authors stay focused on declarative
  workflow definitions and public API usage.
  """

  alias SquidMesh.Runtime.Journal.Options

  @type stale_step_timeout :: non_neg_integer() | :disabled
  @type runtime :: :journal
  @type read_model :: :read_model
  @type raw_config :: [
          repo: module(),
          stale_step_timeout: stale_step_timeout(),
          runtime: runtime(),
          read_model: read_model(),
          journal_storage: term(),
          queue: atom() | String.t()
        ]
  @type t :: %__MODULE__{
          repo: module(),
          stale_step_timeout: stale_step_timeout(),
          runtime: runtime(),
          read_model: read_model(),
          journal_storage: SquidMesh.Runtime.Journal.Storage.t() | nil,
          queue: String.t()
        }

  defstruct [
    :repo,
    :journal_storage,
    stale_step_timeout: :disabled,
    runtime: :journal,
    read_model: :read_model,
    queue: "default"
  ]

  @default_stale_step_timeout :disabled
  @default_runtime :journal
  @default_read_model :read_model
  @default_queue "default"
  @runtimes [:journal]
  @read_models [:read_model]

  @type config_error :: {:missing_config, [atom()]} | {:invalid_config, keyword()}

  @doc """
  Loads Squid Mesh configuration from the host application environment.

  Optional overrides are merged after application configuration so tests and
  embedding applications can supply runtime-specific repositories without
  mutating global application state.
  """
  @spec load(keyword()) :: {:ok, t()} | {:error, config_error()}
  def load(overrides \\ []) do
    config =
      :squid_mesh
      |> Application.get_all_env()
      |> Keyword.merge(overrides)

    with :ok <- validate_required_keys(config),
         {:ok, stale_step_timeout} <-
           validate_stale_step_timeout(
             Keyword.get(config, :stale_step_timeout, @default_stale_step_timeout)
           ),
         {:ok, runtime} <- validate_runtime(Keyword.get(config, :runtime, @default_runtime)),
         {:ok, read_model} <-
           validate_read_model(Keyword.get(config, :read_model, @default_read_model)),
         {:ok, queue} <- validate_queue(Keyword.get(config, :queue, @default_queue)),
         :ok <- reject_legacy_executor(config),
         {:ok, journal_storage} <- validate_journal_storage(config, runtime, read_model) do
      {:ok,
       %__MODULE__{
         repo: Keyword.fetch!(config, :repo),
         stale_step_timeout: stale_step_timeout,
         runtime: runtime,
         read_model: read_model,
         journal_storage: journal_storage,
         queue: queue
       }}
    end
  end

  @doc """
  Loads configuration or raises an `ArgumentError` with the validation details.
  """
  @spec load!(keyword()) :: t()
  def load!(overrides \\ []) do
    case load(overrides) do
      {:ok, config} ->
        config

      {:error, {:missing_config, keys}} ->
        keys = Enum.map_join(keys, ", ", &inspect/1)

        raise ArgumentError,
              "missing Squid Mesh configuration keys: #{keys}"

      {:error, {:invalid_config, details}} ->
        details =
          Enum.map_join(details, ", ", fn {key, value} -> "#{inspect(key)}=#{inspect(value)}" end)

        raise ArgumentError,
              "invalid Squid Mesh configuration: #{details}"
    end
  end

  defp validate_required_keys(config) do
    missing_keys = Enum.reject([:repo], &Keyword.has_key?(config, &1))

    case missing_keys do
      [] -> :ok
      keys -> {:error, {:missing_config, keys}}
    end
  end

  defp validate_stale_step_timeout(stale_step_timeout) do
    case stale_step_timeout do
      :disabled ->
        {:ok, :disabled}

      timeout when is_integer(timeout) and timeout >= 0 ->
        {:ok, timeout}

      invalid ->
        {:error, {:invalid_config, [stale_step_timeout: invalid]}}
    end
  end

  defp validate_runtime(runtime) when runtime in @runtimes, do: {:ok, runtime}

  defp validate_runtime(runtime) do
    {:error, {:invalid_config, [runtime: runtime]}}
  end

  defp validate_read_model(read_model) when read_model in @read_models, do: {:ok, read_model}

  defp validate_read_model(read_model) do
    {:error, {:invalid_config, [read_model: read_model]}}
  end

  defp validate_queue(queue) do
    case Options.queue(queue) do
      {:ok, queue} ->
        {:ok, queue}

      {:error, {:invalid_option, {:queue, :invalid}}} ->
        {:error, {:invalid_config, [queue: :invalid]}}
    end
  end

  defp validate_journal_storage(config, :journal, :read_model) do
    validate_required_journal_storage(config)
  end

  defp validate_required_journal_storage(config) do
    case Keyword.fetch(config, :journal_storage) do
      {:ok, nil} ->
        {:error, {:missing_config, [:journal_storage]}}

      {:ok, storage} ->
        case Options.storage(storage) do
          {:ok, storage} ->
            {:ok, storage}

          {:error, {:invalid_option, {:journal_storage, reason}}} ->
            {:error, {:invalid_config, [journal_storage: reason]}}
        end

      :error ->
        infer_journal_storage(config)
    end
  end

  defp infer_journal_storage(config) do
    config
    |> Keyword.fetch!(:repo)
    |> then(&Options.storage({SquidMesh.Runtime.Journal.Storage.Ecto, repo: &1}))
    |> case do
      {:ok, storage} ->
        {:ok, storage}

      {:error, {:invalid_option, {:journal_storage, reason}}} ->
        {:error, {:invalid_config, [journal_storage: reason]}}
    end
  end

  defp reject_legacy_executor(config) do
    if Keyword.has_key?(config, :executor),
      do: {:error, {:invalid_config, [executor: :unsupported]}},
      else: :ok
  end
end
