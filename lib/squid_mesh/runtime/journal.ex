defmodule SquidMesh.Runtime.Journal do
  @moduledoc """
  Jido.Storage boundary for Squid Mesh durable runtime facts.

  The dispatch protocol owns the runtime fact schema. This module adapts those
  facts into Jido thread entries and checkpoints so storage-backed runtime
  slices can rebuild projections without reading the legacy runtime tables.
  """

  alias Jido.Thread
  alias SquidMesh.Runtime.DispatchProtocol.Entry
  alias SquidMesh.Runtime.DispatchProtocol.Projection
  alias SquidMesh.Runtime.Journal.Checkpoint

  @type storage_config :: module() | {module(), keyword()}
  @type append_error :: :empty_entries | {:mixed_threads, [Entry.thread()]} | term()
  @type loaded_thread :: %{
          thread: Entry.thread(),
          thread_id: String.t(),
          rev: non_neg_integer(),
          entries: [Entry.t()]
        }

  @namespace "squid_mesh"

  @doc false
  @spec append_entries(storage_config(), [Entry.t()], keyword()) ::
          {:ok, Thread.t()} | {:error, append_error()}
  def append_entries(storage, entries, opts \\ [])

  def append_entries(_storage, [], _opts), do: {:error, :empty_entries}

  def append_entries(storage, [%Entry{} | _entries] = entries, opts) when is_list(opts) do
    case entry_thread(entries) do
      {:ok, thread} ->
        {adapter, storage_opts} = Jido.Storage.normalize_storage(storage)
        append_opts = Keyword.merge(storage_opts, opts)

        adapter.append_thread(
          thread_id(thread),
          Enum.map(entries, &to_jido_entry/1),
          append_opts
        )

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec load_entries(storage_config(), Entry.thread()) ::
          {:ok, [Entry.t()]} | {:error, term()}
  def load_entries(storage, thread) do
    with {:ok, %{entries: entries}} <- load_thread(storage, thread) do
      {:ok, entries}
    end
  end

  @doc false
  @spec load_thread(storage_config(), Entry.thread()) :: {:ok, loaded_thread()} | {:error, term()}
  def load_thread(storage, thread) do
    {adapter, opts} = Jido.Storage.normalize_storage(storage)
    thread_id = thread_id(thread)

    with {:ok, %Thread{} = jido_thread} <-
           Jido.Storage.fetch_thread(adapter, thread_id, opts),
         {:ok, entries} <- decode_entries(jido_thread.entries, thread) do
      {:ok,
       %{
         thread: thread,
         thread_id: thread_id,
         rev: jido_thread.rev,
         entries: entries
       }}
    end
  end

  @doc false
  @spec rebuild_dispatch_projection(storage_config(), String.t()) ::
          {:ok, Projection.t()} | {:error, term()}
  def rebuild_dispatch_projection(storage, queue) do
    with {:ok, entries} <- load_entries(storage, {:dispatch, queue}) do
      {:ok, Projection.rebuild(entries)}
    end
  end

  @doc false
  @spec put_checkpoint(storage_config(), Entry.thread(), term(), non_neg_integer(), keyword()) ::
          :ok | {:error, term()}
  def put_checkpoint(storage, thread, projection, thread_rev, opts \\ [])
      when is_integer(thread_rev) and thread_rev >= 0 and is_list(opts) do
    {adapter, storage_opts} = Jido.Storage.normalize_storage(storage)
    thread_id = thread_id(thread)

    checkpoint = %Checkpoint{
      thread: thread,
      thread_id: thread_id,
      thread_rev: thread_rev,
      projection: projection,
      updated_at: Keyword.get(opts, :updated_at, DateTime.utc_now())
    }

    adapter.put_checkpoint(checkpoint_key(thread_id), checkpoint, storage_opts)
  end

  @doc false
  @spec fetch_checkpoint(storage_config(), Entry.thread()) ::
          {:ok, Checkpoint.t()} | {:error, term()}
  def fetch_checkpoint(storage, thread) do
    {adapter, opts} = Jido.Storage.normalize_storage(storage)
    Jido.Storage.fetch_checkpoint(adapter, checkpoint_key(thread_id(thread)), opts)
  end

  @doc false
  @spec thread_id(Entry.thread()) :: String.t()
  def thread_id({:run, run_id}), do: encode_thread_id("run", run_id)
  def thread_id({:dispatch, queue}), do: encode_thread_id("dispatch", queue)
  def thread_id({:run_index, workflow}), do: encode_thread_id("run_index", workflow)

  defp entry_thread(entries) do
    threads =
      entries
      |> Enum.map(& &1.thread)
      |> Enum.uniq()

    case threads do
      [thread] -> {:ok, thread}
      mixed -> {:error, {:mixed_threads, mixed}}
    end
  end

  defp to_jido_entry(%Entry{} = entry) do
    %Jido.Thread.Entry{
      id: nil,
      seq: 0,
      at: datetime_to_millisecond(entry.occurred_at),
      kind: entry.type,
      payload: %{
        data: entry.data,
        occurred_at: entry.occurred_at
      },
      refs: %{
        squid_mesh_thread: entry.thread,
        squid_mesh_thread_id: thread_id(entry.thread)
      }
    }
  end

  defp decode_entries(jido_entries, thread) do
    result =
      Enum.reduce_while(jido_entries, {:ok, []}, fn jido_entry, {:ok, entries} ->
        case from_jido_entry(jido_entry, thread) do
          {:ok, entry} -> {:cont, {:ok, [entry | entries]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp from_jido_entry(%Jido.Thread.Entry{payload: payload} = jido_entry, thread)
       when is_map(payload) do
    with {:ok, data} <- fetch_payload_data(jido_entry),
         {:ok, occurred_at} <- occurred_at(jido_entry) do
      {:ok,
       %Entry{
         type: jido_entry.kind,
         thread: thread,
         data: data,
         occurred_at: occurred_at
       }}
    end
  end

  defp from_jido_entry(%Jido.Thread.Entry{} = jido_entry, _thread) do
    {:error, {:invalid_journal_entry, jido_entry.seq, :invalid_payload}}
  end

  defp fetch_payload_data(%Jido.Thread.Entry{payload: payload} = jido_entry) do
    case Map.fetch(payload, :data) do
      {:ok, data} -> {:ok, data}
      :error -> {:error, {:invalid_journal_entry, jido_entry.seq, :missing_data}}
    end
  end

  defp occurred_at(%Jido.Thread.Entry{payload: payload} = jido_entry) do
    case Map.get(payload, :occurred_at) do
      %DateTime{} = datetime ->
        {:ok, datetime}

      milliseconds when is_integer(milliseconds) ->
        datetime_from_millisecond(jido_entry, milliseconds)

      nil ->
        datetime_from_millisecond(jido_entry, jido_entry.at)

      _invalid ->
        {:error, {:invalid_journal_entry, jido_entry.seq, :invalid_timestamp}}
    end
  end

  defp datetime_from_millisecond(%Jido.Thread.Entry{} = jido_entry, milliseconds)
       when is_integer(milliseconds) do
    case DateTime.from_unix(milliseconds, :millisecond) do
      {:ok, datetime} ->
        {:ok, datetime}

      {:error, _reason} ->
        {:error, {:invalid_journal_entry, jido_entry.seq, :invalid_timestamp}}
    end
  end

  defp datetime_from_millisecond(%Jido.Thread.Entry{} = jido_entry, _invalid) do
    {:error, {:invalid_journal_entry, jido_entry.seq, :invalid_timestamp}}
  end

  defp checkpoint_key(thread_id), do: {@namespace, :checkpoint, thread_id}

  defp encode_thread_id(kind, id), do: Enum.join([@namespace, kind, to_string(id)], ":")

  defp datetime_to_millisecond(%DateTime{} = datetime) do
    DateTime.to_unix(datetime, :millisecond)
  end
end
