defmodule SquidMesh.RunStoreTest do
  use SquidMesh.DataCase

  import Ecto.Query

  alias SquidMesh.Persistence.Run, as: RunRecord
  alias SquidMesh.RunStore

  defmodule LockStepRepo do
    import Ecto.Query

    alias SquidMesh.Persistence.Run, as: RunRecord

    @scenario_key {__MODULE__, :scenario}

    def put_locked_transition(run_id, from_status, to_status, attrs \\ %{}) do
      :persistent_term.put(@scenario_key, %{
        run_id: run_id,
        from_status: from_status,
        to_status: to_status,
        attrs: attrs,
        fired: false
      })
    end

    def clear_locked_transition do
      :persistent_term.erase(@scenario_key)
    end

    def transaction(fun), do: SquidMesh.Test.Repo.transaction(fun)
    def transaction(fun, opts), do: SquidMesh.Test.Repo.transaction(fun, opts)
    def rollback(reason), do: SquidMesh.Test.Repo.rollback(reason)
    def update(changeset), do: SquidMesh.Test.Repo.update(changeset)
    def update(changeset, opts), do: SquidMesh.Test.Repo.update(changeset, opts)

    def one(query), do: maybe_flip_locked_run(query, fn -> SquidMesh.Test.Repo.one(query) end)

    def one(query, opts),
      do: maybe_flip_locked_run(query, fn -> SquidMesh.Test.Repo.one(query, opts) end)

    defp maybe_flip_locked_run(%Ecto.Query{lock: "FOR UPDATE"}, fetch_fun) do
      case :persistent_term.get(@scenario_key, nil) do
        %{
          run_id: run_id,
          from_status: from_status,
          to_status: to_status,
          attrs: attrs,
          fired: false
        } = scenario ->
          current_run = SquidMesh.Test.Repo.get(RunRecord, run_id)

          if current_run && current_run.status == Atom.to_string(from_status) do
            SquidMesh.Test.Repo.update_all(
              from(run_record in RunRecord, where: run_record.id == ^run_id),
              set: locked_transition_attrs(to_status, attrs)
            )

            :persistent_term.put(@scenario_key, %{scenario | fired: true})
          end

          fetch_fun.()

        _other ->
          fetch_fun.()
      end
    end

    defp maybe_flip_locked_run(_query, fetch_fun), do: fetch_fun.()

    defp locked_transition_attrs(to_status, attrs) do
      serialized_attrs =
        attrs
        |> Enum.map(fn
          {:current_step, step} when is_atom(step) -> {:current_step, Atom.to_string(step)}
          pair -> pair
        end)

      [{:status, Atom.to_string(to_status)} | serialized_attrs]
    end
  end

  defmodule InvoiceReminderWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :invoice_delivery do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :load_invoice, InvoiceReminderWorkflow.LoadInvoice, retry: [max_attempts: 1]
      transition :load_invoice, on: :ok, to: :complete
    end
  end

  defmodule MultiTriggerWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_digest do
        manual()

        payload do
          field :chat_id, :integer
        end
      end

      trigger :scheduled_digest do
        cron "0 9 * * *", timezone: "UTC"

        payload do
          field :window_start_at, :string, default: {:today, :iso8601}
        end
      end

      step :deliver_digest, MultiTriggerWorkflow.DeliverDigest
      transition :deliver_digest, on: :ok, to: :complete
    end
  end

  describe "create_and_dispatch_run/5" do
    test "keeps only reserved run-level facts from initial context" do
      schedule = %{
        trigger_name: "scheduled_digest",
        cron_expression: "0 9 * * *",
        timezone: "UTC",
        signal_id: "signal_123",
        received_at: "2026-05-15T10:15:00Z",
        intended_window: %{
          start_at: "2026-05-15T09:00:00Z",
          end_at: "2026-05-15T10:00:00Z"
        }
      }

      assert {:ok, run} =
               RunStore.create_and_dispatch_run(
                 Repo,
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123"},
                 fn _run -> {:ok, :noop} end,
                 initial_context: %{attempt: 1, schedule: schedule}
               )

      assert run.context == %{schedule: schedule}
    end
  end

  describe "transition_run/4" do
    test "persists a valid transition" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, transitioned_run} =
               RunStore.transition_run(Repo, run.id, :running, %{current_step: :load_invoice})

      assert transitioned_run.id == run.id
      assert transitioned_run.trigger == :invoice_delivery
      assert transitioned_run.status == :running
      assert transitioned_run.current_step == :load_invoice
    end

    test "persists transition metadata alongside the status change" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      error = %{message: "gateway timeout"}

      assert {:ok, transitioned_run} =
               RunStore.transition_run(Repo, run.id, :failed, %{
                 last_error: error,
                 context: %{attempt: 3}
               })

      assert transitioned_run.status == :failed
      assert transitioned_run.last_error == error
      assert transitioned_run.context == %{attempt: 3}
    end

    test "rejects invalid transitions and keeps the persisted state unchanged" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:error, {:invalid_transition, :pending, :completed}} =
               RunStore.transition_run(Repo, run.id, :completed)

      assert {:ok, persisted_run} = RunStore.get_run(Repo, run.id)

      assert persisted_run.status == :pending
      assert persisted_run.current_step == :load_invoice
    end

    test "returns not found when the run does not exist" do
      assert {:error, :not_found} =
               RunStore.transition_run(Repo, Ecto.UUID.generate(), :running)
    end

    test "creates a run through an explicit trigger name" do
      assert {:ok, run} =
               RunStore.create_run(
                 Repo,
                 InvoiceReminderWorkflow,
                 :invoice_delivery,
                 %{account_id: "acct_123"}
               )

      assert run.trigger == :invoice_delivery
    end

    test "validates payload against the selected trigger" do
      assert {:ok, manual_run} =
               RunStore.create_run(Repo, MultiTriggerWorkflow, :manual_digest, %{chat_id: 123})

      assert manual_run.trigger == :manual_digest
      assert manual_run.payload == %{chat_id: 123}

      assert {:ok, scheduled_run} =
               RunStore.create_run(Repo, MultiTriggerWorkflow, :scheduled_digest, %{})

      assert scheduled_run.trigger == :scheduled_digest
      assert is_binary(scheduled_run.payload.window_start_at)

      assert {:ok, reloaded_scheduled_run} = RunStore.get_run(Repo, scheduled_run.id)
      assert reloaded_scheduled_run.payload == scheduled_run.payload

      assert {:error, {:invalid_payload, %{missing_fields: [:chat_id]}}} =
               RunStore.create_run(Repo, MultiTriggerWorkflow, :manual_digest, %{})
    end
  end

  describe "cancel_run/2" do
    test "cancels pending runs immediately" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, cancelled_run} = RunStore.cancel_run(Repo, run.id)

      assert cancelled_run.status == :cancelled
      assert RunStore.schedule_next_step?(cancelled_run) == false
    end

    test "marks active runs as cancelling and prevents future scheduling" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, running_run} = RunStore.transition_run(Repo, run.id, :running)
      assert {:ok, cancelling_run} = RunStore.cancel_run(Repo, running_run.id)

      assert cancelling_run.status == :cancelling
      assert RunStore.schedule_next_step?(cancelling_run) == false
    end

    test "cancels paused runs immediately" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, running_run} = RunStore.transition_run(Repo, run.id, :running)

      assert {:ok, paused_run} =
               RunStore.transition_run(Repo, running_run.id, :paused, %{
                 current_step: :wait_for_approval
               })

      assert {:ok, cancelled_run} = RunStore.cancel_run(Repo, paused_run.id)

      assert cancelled_run.status == :cancelled
      assert RunStore.schedule_next_step?(cancelled_run) == false
    end

    test "rejects cancellation for terminal runs" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, failed_run} = RunStore.transition_run(Repo, run.id, :failed)

      assert {:error, {:invalid_transition, :failed, :cancelling}} =
               RunStore.cancel_run(Repo, failed_run.id)
    end

    test "cancels a run that becomes paused before cancellation acquires the lock" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, running_run} =
               RunStore.transition_run(Repo, run.id, :running, %{current_step: :load_invoice})

      LockStepRepo.put_locked_transition(
        run.id,
        :running,
        :paused,
        %{current_step: :wait_for_approval}
      )

      on_exit(fn -> LockStepRepo.clear_locked_transition() end)

      assert {:ok, cancelled_run} = RunStore.cancel_run(LockStepRepo, running_run.id)
      assert cancelled_run.status == :cancelled
    end

    test "cancels a paused run that becomes running before cancellation acquires the lock" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, running_run} =
               RunStore.transition_run(Repo, run.id, :running, %{current_step: :load_invoice})

      assert {:ok, paused_run} =
               RunStore.transition_run(Repo, running_run.id, :paused, %{
                 current_step: :wait_for_approval
               })

      LockStepRepo.put_locked_transition(
        run.id,
        :paused,
        :running,
        %{current_step: :load_invoice}
      )

      on_exit(fn -> LockStepRepo.clear_locked_transition() end)

      assert {:ok, cancelling_run} = RunStore.cancel_run(LockStepRepo, paused_run.id)
      assert cancelling_run.status == :cancelling
    end

    test "clears current_step when cancelling a paused run" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, running_run} =
               RunStore.transition_run(Repo, run.id, :running, %{current_step: :load_invoice})

      assert {:ok, paused_run} =
               RunStore.transition_run(Repo, running_run.id, :paused, %{
                 current_step: :wait_for_approval
               })

      assert {:ok, cancelled_run} = RunStore.cancel_run(Repo, paused_run.id)
      assert cancelled_run.status == :cancelled
      assert is_nil(cancelled_run.current_step)
    end
  end

  describe "get_run/2" do
    test "returns stable workflow and step identifiers after reloading from persistence" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      persisted_run = Repo.get!(RunRecord, run.id)

      assert persisted_run.workflow == "Elixir.SquidMesh.RunStoreTest.InvoiceReminderWorkflow"
      assert persisted_run.current_step == "load_invoice"

      assert {:ok, loaded_run} = RunStore.get_run(Repo, run.id)

      assert loaded_run.workflow == InvoiceReminderWorkflow
      assert loaded_run.trigger == :invoice_delivery
      assert loaded_run.current_step == :load_invoice
    end

    test "returns a structured error for malformed run ids" do
      assert {:error, :invalid_run_id} = RunStore.get_run(Repo, "not-a-uuid")
    end
  end

  describe "replay_run/2" do
    test "creates a distinct pending run linked to the source run" do
      payload = %{account_id: "acct_123"}

      assert {:ok, source_run} = RunStore.create_run(Repo, InvoiceReminderWorkflow, payload)

      assert {:ok, replay_run} = RunStore.replay_run(Repo, source_run.id)

      assert replay_run.id != source_run.id
      assert replay_run.workflow == source_run.workflow
      assert replay_run.trigger == :invoice_delivery
      assert replay_run.status == :pending
      assert replay_run.payload == payload
      assert replay_run.context == %{}
      assert replay_run.current_step == :load_invoice
      assert replay_run.last_error == nil
      assert replay_run.replayed_from_run_id == source_run.id
    end

    test "leaves the source run unchanged" do
      assert {:ok, source_run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, failed_run} =
               RunStore.transition_run(Repo, source_run.id, :failed, %{
                 current_step: :load_invoice,
                 context: %{attempt: 1},
                 last_error: %{message: "timeout"}
               })

      assert {:ok, replay_run} = RunStore.replay_run(Repo, failed_run.id)
      assert {:ok, persisted_source_run} = RunStore.get_run(Repo, failed_run.id)

      assert replay_run.replayed_from_run_id == failed_run.id
      assert persisted_source_run == failed_run
    end

    test "preserves scheduled start metadata while dropping step-derived context" do
      schedule = %{
        trigger_name: "scheduled_digest",
        cron_expression: "0 9 * * *",
        timezone: "UTC",
        signal_id: "signal_123",
        received_at: "2026-05-15T10:15:00Z",
        intended_window: %{
          start_at: "2026-05-15T09:00:00Z",
          end_at: "2026-05-15T10:00:00Z"
        }
      }

      assert {:ok, source_run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, failed_run} =
               RunStore.transition_run(Repo, source_run.id, :failed, %{
                 current_step: :load_invoice,
                 context: %{attempt: 1, schedule: schedule},
                 last_error: %{message: "timeout"}
               })

      assert {:ok, replay_run} = RunStore.replay_run(Repo, failed_run.id)

      assert replay_run.context == %{schedule: schedule}
      refute Map.has_key?(replay_run.context, :attempt)
    end

    test "drops schedule idempotency from replayed runs" do
      schedule = %{
        trigger_name: "scheduled_digest",
        cron_expression: "0 9 * * *",
        timezone: "UTC",
        signal_id: "signal_123",
        idempotency: "return_existing_run",
        idempotency_key: "signal_123",
        received_at: "2026-05-15T10:15:00Z"
      }

      assert {:ok, source_run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, failed_run} =
               RunStore.transition_run(Repo, source_run.id, :failed, %{
                 current_step: :load_invoice,
                 context: %{schedule: schedule},
                 last_error: %{message: "timeout"}
               })

      assert {:ok, replay_run} = RunStore.replay_run(Repo, failed_run.id)

      assert replay_run.context.schedule.signal_id == "signal_123"
      refute Map.has_key?(replay_run.context.schedule, :idempotency)
      refute Map.has_key?(replay_run.context.schedule, :idempotency_key)
    end

    test "returns not found when the source run does not exist" do
      assert {:error, :not_found} = RunStore.replay_run(Repo, Ecto.UUID.generate())
    end

    test "returns a structured error for malformed run ids" do
      assert {:error, :invalid_run_id} = RunStore.replay_run(Repo, "not-a-uuid")
    end
  end

  describe "progress_run_with/4" do
    test "does not update or dispatch terminal runs" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, failed_run} =
               RunStore.transition_run(Repo, run.id, :failed, %{
                 current_step: :load_invoice,
                 context: %{attempt: 1},
                 last_error: %{message: "timeout"}
               })

      assert {:ok, :noop} =
               RunStore.progress_run_with(
                 Repo,
                 failed_run.id,
                 fn _current_run ->
                   %{
                     context: %{attempt: 2},
                     current_step: nil,
                     last_error: nil
                   }
                 end,
                 {:dispatch,
                  fn _updated_run ->
                    send(self(), :dispatched)
                    {:ok, :sent}
                  end}
               )

      refute_received :dispatched
      assert {:ok, persisted_run} = RunStore.get_run(Repo, failed_run.id)
      assert persisted_run == failed_run
    end

    test "finalizes cancelling runs without dispatching more work" do
      assert {:ok, run} =
               RunStore.create_run(Repo, InvoiceReminderWorkflow, %{account_id: "acct_123"})

      assert {:ok, running_run} =
               RunStore.transition_run(Repo, run.id, :running, %{
                 current_step: :load_invoice
               })

      assert {:ok, cancelling_run} = RunStore.cancel_run(Repo, running_run.id)

      assert {:ok, cancelled_run} =
               RunStore.progress_run_with(
                 Repo,
                 cancelling_run.id,
                 fn current_run ->
                   %{
                     context: Map.put(current_run.context, :delivered, true),
                     current_step: :load_invoice,
                     last_error: %{message: "ignored"}
                   }
                 end,
                 {:dispatch,
                  fn _updated_run ->
                    send(self(), :dispatched)
                    {:ok, :sent}
                  end}
               )

      refute_received :dispatched
      assert cancelled_run.status == :cancelled
      assert cancelled_run.current_step == nil
      assert cancelled_run.last_error == nil
      assert cancelled_run.context == %{delivered: true}
    end
  end
end
