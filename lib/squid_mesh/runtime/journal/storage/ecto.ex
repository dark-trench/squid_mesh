defmodule SquidMesh.Runtime.Journal.Storage.Ecto do
  @moduledoc """
  Postgres-compatible Ecto storage adapter for Squid Mesh journal runtime state.

  Use this adapter when the host app wants the Jido journal runtime persisted in
  the same Postgres-compatible database boundary as the rest of the application:

      config :squid_mesh,
        runtime: :journal,
        read_model: :read_model,
        journal_storage: {SquidMesh.Runtime.Journal.Storage.Ecto, repo: MyApp.Repo}

  The adapter implements Jido's checkpoint and append-only thread callbacks.
  Thread appends are serialized with a row-level lock and honor Jido's
  `:expected_rev` optimistic concurrency option.
  """

  @behaviour Jido.Storage

  import Ecto.Query

  alias Jido.Thread
  alias Jido.Thread.EntryNormalizer
  alias SquidMesh.Persistence.JournalCheckpoint
  alias SquidMesh.Persistence.JournalEntry
  alias SquidMesh.Persistence.JournalThread

  @type opts :: keyword()

  @impl Jido.Storage
  @spec get_checkpoint(term(), opts()) :: {:ok, term()} | :not_found | {:error, term()}
  def get_checkpoint(key, opts) do
    with {:ok, repo} <- fetch_repo(opts),
         {:ok, key_binary} <- encode_term(key) do
      case repo.get(JournalCheckpoint, key_hash(key_binary), repo_opts(opts)) do
        nil -> :not_found
        checkpoint -> decode_term(checkpoint.checkpoint)
      end
    end
  end

  @impl Jido.Storage
  @spec put_checkpoint(term(), term(), opts()) :: :ok | {:error, term()}
  def put_checkpoint(key, data, opts) do
    with {:ok, repo} <- fetch_repo(opts),
         {:ok, key_binary} <- encode_term(key),
         {:ok, checkpoint_binary} <- encode_term(data) do
      now = DateTime.utc_now(:microsecond)

      row = %{
        key_hash: key_hash(key_binary),
        key: key_binary,
        checkpoint: checkpoint_binary,
        inserted_at: now,
        updated_at: now
      }

      {_count, _rows} =
        repo.insert_all(
          JournalCheckpoint,
          [row],
          [on_conflict: {:replace, [:key, :checkpoint, :updated_at]}, conflict_target: :key_hash] ++
            repo_opts(opts)
        )

      :ok
    end
  end

  @impl Jido.Storage
  @spec delete_checkpoint(term(), opts()) :: :ok | {:error, term()}
  def delete_checkpoint(key, opts) do
    with {:ok, repo} <- fetch_repo(opts),
         {:ok, key_binary} <- encode_term(key) do
      repo.delete_all(
        from(checkpoint in JournalCheckpoint,
          where: checkpoint.key_hash == ^key_hash(key_binary)
        ),
        repo_opts(opts)
      )

      :ok
    end
  end

  @impl Jido.Storage
  @spec load_thread(String.t(), opts()) :: {:ok, Thread.t()} | :not_found | {:error, term()}
  def load_thread(thread_id, opts) when is_binary(thread_id) do
    with {:ok, repo} <- fetch_repo(opts) do
      result = repo.transaction(fn -> load_thread_in_transaction(repo, thread_id, opts) end)
      normalize_load_thread_result(result)
    end
  end

  @impl Jido.Storage
  @spec append_thread(String.t(), [Jido.Thread.Entry.t()], opts()) ::
          {:ok, Thread.t()} | {:error, term()}
  def append_thread(thread_id, entries, opts) when is_binary(thread_id) and is_list(entries) do
    with {:ok, repo} <- fetch_repo(opts) do
      expected_rev = Keyword.get(opts, :expected_rev)
      now_ms = System.system_time(:millisecond)

      result =
        repo.transaction(fn ->
          append_thread_in_transaction(repo, thread_id, entries, expected_rev, now_ms, opts)
        end)

      normalize_thread_result(result)
    end
  end

  @impl Jido.Storage
  @spec delete_thread(String.t(), opts()) :: :ok | {:error, term()}
  def delete_thread(thread_id, opts) when is_binary(thread_id) do
    with {:ok, repo} <- fetch_repo(opts) do
      repo.delete_all(
        from(entry in JournalEntry, where: entry.thread_id == ^thread_id),
        repo_opts(opts)
      )

      repo.delete_all(
        from(thread in JournalThread, where: thread.id == ^thread_id),
        repo_opts(opts)
      )

      :ok
    end
  end

  defp fetch_repo(opts) do
    case Keyword.get(opts, :repo) do
      repo when is_atom(repo) and not is_nil(repo) ->
        if Code.ensure_loaded?(repo) and function_exported?(repo, :transaction, 1) do
          {:ok, repo}
        else
          {:error, {:invalid_option, :repo}}
        end

      _invalid ->
        {:error, {:missing_option, :repo}}
    end
  end

  defp load_thread_in_transaction(repo, thread_id, opts) do
    case locked_thread_for_read(repo, thread_id, opts) do
      nil -> repo.rollback(:not_found)
      %JournalThread{} = thread -> load_locked_thread(repo, thread, opts)
    end
  end

  defp load_locked_thread(repo, %JournalThread{} = thread, opts) do
    case load_entries(repo, thread.id, opts) do
      {:ok, []} -> repo.rollback(:not_found)
      {:ok, entries} -> reconstruct_or_rollback(repo, thread, entries)
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp reconstruct_or_rollback(repo, %JournalThread{} = thread, entries) do
    case validate_and_reconstruct_thread(thread, entries) do
      %Thread{} = reconstructed_thread -> reconstructed_thread
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp normalize_load_thread_result({:ok, %Thread{} = thread}), do: {:ok, thread}
  defp normalize_load_thread_result({:error, :not_found}), do: :not_found
  defp normalize_load_thread_result({:error, reason}), do: {:error, reason}

  defp append_thread_in_transaction(repo, thread_id, entries, expected_rev, now_ms, opts) do
    thread = ensure_locked_thread(repo, thread_id, now_ms, opts)

    case validate_expected_rev(expected_rev, thread.rev) do
      :ok -> append_locked_thread(repo, thread, entries, now_ms, opts)
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp append_locked_thread(repo, %JournalThread{} = thread, entries, now_ms, opts) do
    prepared_entries = EntryNormalizer.normalize_many(entries, thread.rev, now_ms)

    with :ok <- insert_entries(repo, thread.id, prepared_entries, opts),
         %JournalThread{} = updated_thread <-
           update_thread_revision(
             repo,
             thread,
             thread.rev + length(prepared_entries),
             now_ms,
             opts
           ),
         {:ok, all_entries} <- load_entries(repo, thread.id, opts) do
      reconstruct_thread(updated_thread, all_entries)
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp normalize_thread_result({:ok, %Thread{} = thread}), do: {:ok, thread}
  defp normalize_thread_result({:error, reason}), do: {:error, reason}

  defp validate_expected_rev(nil, _current_rev), do: :ok
  defp validate_expected_rev(current_rev, current_rev), do: :ok
  defp validate_expected_rev(_expected_rev, _current_rev), do: {:error, :conflict}

  defp ensure_locked_thread(repo, thread_id, now_ms, opts) do
    db_now = DateTime.utc_now(:microsecond)

    row = %{
      id: thread_id,
      rev: 0,
      metadata: Keyword.get(opts, :metadata, %{}),
      created_at_ms: now_ms,
      updated_at_ms: now_ms,
      inserted_at: db_now,
      updated_at: db_now
    }

    repo.insert_all(
      JournalThread,
      [row],
      [on_conflict: :nothing, conflict_target: :id] ++ repo_opts(opts)
    )

    locked_thread_for_update(repo, thread_id, opts)
  end

  defp locked_thread_for_read(repo, thread_id, opts) do
    repo.one(
      from(thread in JournalThread, where: thread.id == ^thread_id, lock: "FOR SHARE"),
      repo_opts(opts)
    )
  end

  defp locked_thread_for_update(repo, thread_id, opts) do
    repo.one(
      from(thread in JournalThread, where: thread.id == ^thread_id, lock: "FOR UPDATE"),
      repo_opts(opts)
    )
  end

  defp insert_entries(_repo, _thread_id, [], _opts), do: :ok

  defp insert_entries(repo, thread_id, entries, opts) do
    now = DateTime.utc_now(:microsecond)

    rows =
      Enum.map(entries, fn entry ->
        {:ok, entry_binary} = encode_term(entry)

        %{
          id: Ecto.UUID.generate(),
          thread_id: thread_id,
          seq: entry.seq,
          entry: entry_binary,
          inserted_at: now,
          updated_at: now
        }
      end)

    case repo.insert_all(JournalEntry, rows, repo_opts(opts)) do
      {count, _rows} when count == length(rows) -> :ok
      {count, _rows} -> {:error, {:entries_not_inserted, count, length(rows)}}
    end
  end

  defp update_thread_revision(repo, %JournalThread{} = thread, rev, now_ms, opts) do
    db_now = DateTime.utc_now(:microsecond)

    {1, _rows} =
      repo.update_all(
        from(stored_thread in JournalThread, where: stored_thread.id == ^thread.id),
        [set: [rev: rev, updated_at_ms: now_ms, updated_at: db_now]],
        repo_opts(opts)
      )

    %JournalThread{thread | rev: rev, updated_at_ms: now_ms, updated_at: db_now}
  end

  defp load_entries(repo, thread_id, opts) do
    entries =
      repo.all(
        from(entry in JournalEntry, where: entry.thread_id == ^thread_id, order_by: entry.seq),
        repo_opts(opts)
      )

    result =
      Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, decoded_entries} ->
        case decode_entry(entry.entry) do
          {:ok, decoded_entry} -> {:cont, {:ok, [decoded_entry | decoded_entries]}}
          {:error, reason} -> {:halt, {:error, {:invalid_journal_entry, entry.seq, reason}}}
        end
      end)

    case result do
      {:ok, decoded_entries} -> {:ok, Enum.reverse(decoded_entries)}
      {:error, _reason} = error -> error
    end
  end

  defp reconstruct_thread(%JournalThread{} = thread, entries) do
    %Thread{
      id: thread.id,
      rev: thread.rev,
      entries: entries,
      created_at: thread.created_at_ms || (List.first(entries) && List.first(entries).at),
      updated_at: thread.updated_at_ms || (List.last(entries) && List.last(entries).at),
      metadata: thread.metadata || %{},
      stats: %{entry_count: length(entries)}
    }
  end

  defp validate_and_reconstruct_thread(%JournalThread{} = thread, entries) do
    with :ok <- validate_thread_revision(thread, entries),
         :ok <- validate_entry_sequences(thread, entries) do
      reconstruct_thread(thread, entries)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_thread_revision(%JournalThread{} = thread, entries) do
    entry_count = length(entries)

    if thread.rev == entry_count do
      :ok
    else
      {:error, {:invalid_journal_thread, thread.id, {:rev_mismatch, thread.rev, entry_count}}}
    end
  end

  defp validate_entry_sequences(%JournalThread{} = thread, entries) do
    sequences = Enum.map(entries, & &1.seq)
    expected_sequences = Enum.to_list(0..(length(entries) - 1)//1)

    if sequences == expected_sequences do
      :ok
    else
      {:error, {:invalid_journal_thread, thread.id, {:seq_gap, sequences}}}
    end
  end

  defp repo_opts(opts) do
    case Keyword.fetch(opts, :prefix) do
      {:ok, prefix} -> [prefix: prefix]
      :error -> []
    end
  end

  defp encode_term(term), do: {:ok, :erlang.term_to_binary(term)}

  defp decode_entry(binary) when is_binary(binary) do
    case decode_term(binary) do
      {:ok, %Jido.Thread.Entry{} = entry} -> {:ok, entry}
      {:ok, _invalid} -> {:error, :invalid_entry}
      {:error, _reason} = error -> error
    end
  end

  defp decode_term(binary) when is_binary(binary) do
    # Persisted Jido entries may contain atom keys from workflow definitions and
    # internal metadata that are not loaded yet after a VM restart. This adapter
    # decodes only rows from the configured journal tables and validates the
    # expected entry/checkpoint shape at the boundary after decoding.
    {:ok, :erlang.binary_to_term(binary)}
  rescue
    error -> {:error, {error.__struct__, Exception.message(error)}}
  end

  defp key_hash(key_binary) do
    Base.encode16(:crypto.hash(:sha256, key_binary), case: :lower)
  end
end
