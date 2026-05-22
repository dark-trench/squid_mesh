defmodule SquidMesh.Runtime.JournalOptions do
  @moduledoc """
  Validates public options that reach Jido-backed journal threads.

  Journal-backed start, inspection, and explanation all cross the same storage
  boundary: caller-provided storage config is normalized into a Jido adapter,
  and caller-provided run or queue identifiers become part of journal thread
  ids. This module keeps that validation in one place so those public APIs fail
  with structured, redacted errors before invalid values can reach an adapter or
  file-backed thread path.

  The validation is intentionally about runtime safety, not compatibility. It
  accepts the current Jido storage adapter shape, validates the built-in
  adapters whose required options can otherwise raise, and keeps thread id
  components to a conservative portable character set.
  """

  @thread_part_pattern ~r/^[A-Za-z0-9][A-Za-z0-9_.-]*$/
  @storage_callbacks [
    get_checkpoint: 2,
    put_checkpoint: 3,
    delete_checkpoint: 2,
    load_thread: 2,
    append_thread: 3,
    delete_thread: 2
  ]

  @doc """
  Validates a Jido storage config before a public API calls the adapter.

  Invalid configs return structured, redacted option errors. Built-in adapters
  that raise for missing required options are checked here first.
  """
  @spec storage(term()) :: {:ok, term()} | {:error, {:invalid_option, term()}}
  def storage(nil), do: {:error, {:invalid_option, {:journal_storage, nil}}}

  def storage(storage) when is_atom(storage) do
    validate_storage_module(storage, storage, [])
  end

  def storage({module, opts} = storage) when is_atom(module) and is_list(opts) do
    validate_storage_module(module, storage, opts)
  end

  def storage(storage), do: invalid_storage(storage)

  @doc """
  Normalizes and validates a dispatch queue name for use in journal thread ids.

  Atoms are converted to strings. Queue names must be non-empty and use the
  conservative portable thread-id character set enforced by this module.
  """
  @spec queue(term()) :: {:ok, String.t()} | {:error, {:invalid_option, {:queue, term()}}}
  def queue(queue \\ "default")

  def queue(queue) when is_atom(queue),
    do: validate_thread_part(Atom.to_string(queue), :queue, queue)

  def queue(queue) when is_binary(queue), do: validate_thread_part(queue, :queue, queue)
  def queue(queue), do: {:error, {:invalid_option, {:queue, queue}}}

  @doc """
  Validates a caller-provided journal thread-id component.

  Use this for public identifiers, such as run ids in projection reads, that are
  not required to be UUIDs but still become part of a Jido thread id.
  """
  @spec thread_part(term(), atom()) :: {:ok, String.t()} | {:error, {:invalid_option, term()}}
  def thread_part(value, field) when is_binary(value) and is_atom(field) do
    validate_thread_part(value, field, value)
  end

  def thread_part(value, field) when is_atom(field) do
    {:error, {:invalid_option, {field, value}}}
  end

  @doc """
  Validates and canonicalizes a public run id that must be a UUID.

  Journal starts use UUID run ids so caller-provided ids are safe as stable
  workflow thread identifiers and duplicate-start fences.
  """
  @spec uuid(term()) :: {:ok, String.t()} | {:error, {:invalid_option, {:run_id, term()}}}
  def uuid(value) when is_binary(value) do
    case Ecto.UUID.cast(value) do
      {:ok, uuid} -> {:ok, uuid}
      :error -> {:error, {:invalid_option, {:run_id, value}}}
    end
  end

  def uuid(value), do: {:error, {:invalid_option, {:run_id, value}}}

  defp validate_thread_part("", field, original) do
    {:error, {:invalid_option, {field, original}}}
  end

  defp validate_thread_part(value, field, original) do
    if Regex.match?(@thread_part_pattern, value) do
      {:ok, value}
    else
      {:error, {:invalid_option, {field, original}}}
    end
  end

  defp validate_storage_module(module, storage, opts) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- storage_callbacks?(module),
         :ok <- validate_storage_options(module, opts) do
      {:ok, storage}
    else
      _error -> invalid_storage(storage)
    end
  end

  defp storage_callbacks?(module) do
    Enum.all?(@storage_callbacks, fn {name, arity} ->
      function_exported?(module, name, arity)
    end)
  end

  defp validate_storage_options(Jido.Storage.File, opts) do
    case Keyword.get(opts, :path) do
      path when is_binary(path) and path != "" -> :ok
      _invalid -> :error
    end
  end

  defp validate_storage_options(Jido.Storage.Redis, opts) do
    case Keyword.get(opts, :command_fn) do
      command_fn when is_function(command_fn, 1) -> :ok
      _invalid -> :error
    end
  end

  defp validate_storage_options(_module, _opts), do: :ok

  defp invalid_storage({module, _opts}) when is_atom(module) do
    {:error, {:invalid_option, {:journal_storage, module}}}
  end

  defp invalid_storage(module) when is_atom(module) do
    {:error, {:invalid_option, {:journal_storage, module}}}
  end

  defp invalid_storage(_storage) do
    {:error, {:invalid_option, {:journal_storage, :invalid}}}
  end
end
