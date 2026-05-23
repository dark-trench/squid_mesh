defmodule SquidMesh.Runtime.StepWorkerTest do
  use SquidMesh.DataCase, async: false

  import Ecto.Query

  alias __MODULE__.ApprovalWorkflow
  alias __MODULE__.BackoffWorkflow
  alias __MODULE__.BuiltInWorkflow
  alias __MODULE__.CancellationCompletionWorkflow
  alias __MODULE__.CompensationRetryWorkflow
  alias __MODULE__.CompensationWorkflow
  alias __MODULE__.CompleteUndoRoutingWorkflow
  alias __MODULE__.ConcurrentDependencyFailureWorkflow
  alias __MODULE__.ConcurrentDependencyWorkflow
  alias __MODULE__.ConcurrentRetryWorkflow
  alias __MODULE__.DependencyFailureWorkflow
  alias __MODULE__.DependencyWorkflow
  alias __MODULE__.ErrorRoutingWorkflow
  alias __MODULE__.ExhaustedRetryWorkflow
  alias __MODULE__.ExplicitMappingWorkflow
  alias __MODULE__.FailingWorkflow
  alias __MODULE__.InputIsolationWorkflow
  alias __MODULE__.MissingExecutor
  alias __MODULE__.MissingPathMappingWorkflow
  alias __MODULE__.NamedPathMappingWorkflow
  alias __MODULE__.NativeNamedPathMappingWorkflow
  alias __MODULE__.NativeStepErrorRoutingWorkflow
  alias __MODULE__.NativeStepInputValidationWorkflow
  alias __MODULE__.NativeStepOutputValidationWorkflow
  alias __MODULE__.NativeStepRetryAfterWorkflow
  alias __MODULE__.NativeStepRetryWorkflow
  alias __MODULE__.NativeStepScheduledSuccessWorkflow
  alias __MODULE__.NativeStepWorkflow
  alias __MODULE__.OrderedDependencyWorkflow
  alias __MODULE__.PauseMappedWorkflow
  alias __MODULE__.PauseWorkflow
  alias __MODULE__.RetrySurfaceWorkflow
  alias __MODULE__.SuccessfulWorkflow
  alias __MODULE__.SuccessMissingPathMappingWorkflow
  alias __MODULE__.TransactionalFailureWorkflow
  alias __MODULE__.TransactionalSuccessWorkflow
  alias __MODULE__.UndoRoutingWorkflow

  alias SquidMesh.AttemptStore
  alias SquidMesh.Config
  alias SquidMesh.Persistence.StepRun
  alias SquidMesh.Runtime.Dispatcher
  alias SquidMesh.Runtime.StepExecutor
  alias SquidMesh.Runtime.StepExecutor.Outcome
  alias SquidMesh.Runtime.StepExecutor.Preparation
  alias SquidMesh.Steps
  alias SquidMesh.Test.Job
  alias SquidMesh.Test.StepWorker

  defmodule FailedTransitionRepo do
    alias SquidMesh.Persistence.Run
    alias SquidMesh.Test.Repo

    @spec transaction((-> term())) :: term()
    def transaction(fun), do: Repo.transaction(fun)
    @spec transaction((-> term()), keyword()) :: term()
    def transaction(fun, opts), do: Repo.transaction(fun, opts)
    @spec rollback(term()) :: no_return()
    def rollback(reason), do: Repo.rollback(reason)
    @spec one(Ecto.Queryable.t()) :: term()
    def one(query), do: Repo.one(query)
    @spec one(Ecto.Queryable.t(), keyword()) :: term()
    def one(query, opts), do: Repo.one(query, opts)
    @spec get(module(), term()) :: term()
    def get(schema, id), do: Repo.get(schema, id)
    @spec get!(module(), term()) :: term()
    def get!(schema, id), do: Repo.get!(schema, id)
    @spec insert_all(module(), [map()], keyword()) :: term()
    def insert_all(schema, entries, opts), do: Repo.insert_all(schema, entries, opts)
    @spec update_all(Ecto.Queryable.t(), keyword()) :: term()
    def update_all(query, updates), do: Repo.update_all(query, updates)

    @spec update(Ecto.Changeset.t()) :: term()
    def update(%Ecto.Changeset{data: %Run{}} = changeset) do
      case Ecto.Changeset.fetch_change(changeset, :status) do
        {:ok, "failed"} -> {:error, Ecto.Changeset.add_error(changeset, :status, "forced")}
        _other -> Repo.update(changeset)
      end
    end

    def update(changeset), do: Repo.update(changeset)
  end

  describe "workflow execution through the configured executor" do
    test "enqueues and executes the declared steps through Jido-backed actions" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(SuccessfulWorkflow, input, repo: Repo)

      assert_enqueued(
        worker: SquidMesh.Test.StepWorker,
        queue: "squid_mesh",
        args: %{"run_id" => run.id, "step" => "load_invoice"}
      )

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} = SquidMesh.inspect_run(run.id, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.current_step == nil
      assert completed_run.last_error == nil
      assert completed_run.context.account == %{id: "acct_123"}
      assert completed_run.context.invoice == %{id: "inv_456", status: "open"}

      assert completed_run.context.delivery == %{
               account_id: "acct_123",
               invoice_id: "inv_456",
               channel: "email"
             }

      step_runs =
        Repo.all(
          from(step_run in StepRun,
            where: step_run.run_id == ^run.id,
            order_by: [asc: step_run.inserted_at]
          )
        )

      assert Enum.map(step_runs, &{&1.step, &1.status}) == [
               {"load_invoice", "completed"},
               {"send_email", "completed"}
             ]

      assert Enum.map(step_runs, &AttemptStore.attempt_count(Repo, &1.id)) == [1, 1]
    end

    test "holds a join step until all declared dependencies complete" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(DependencyWorkflow, input, repo: Repo)
      assert run.current_step == nil

      assert 2 ==
               SquidMesh.Test.Executor.available_count(run.id)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "load_account"}
               })

      assert {:ok, running_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert running_run.status == :running
      assert running_run.current_step == nil
      assert running_run.context.account == %{id: "acct_123", tier: "pro"}
      refute Map.has_key?(running_run.context, :delivery)

      assert 1 ==
               SquidMesh.Test.Executor.available_count(run.id, "load_invoice")

      assert 0 ==
               SquidMesh.Test.Executor.available_count(run.id, "send_email")

      assert %{success: 3, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} = SquidMesh.inspect_run(run.id, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.current_step == nil
      assert completed_run.context.account == %{id: "acct_123", tier: "pro"}
      assert completed_run.context.invoice == %{id: "inv_456", status: "open"}

      assert completed_run.context.delivery == %{
               account_id: "acct_123",
               invoice_id: "inv_456",
               channel: "email"
             }

      step_runs =
        Repo.all(
          from(step_run in StepRun,
            where: step_run.run_id == ^run.id,
            order_by: [asc: step_run.inserted_at]
          )
        )

      assert Enum.map(step_runs, &{&1.step, &1.status}) == [
               {"load_account", "completed"},
               {"load_invoice", "completed"},
               {"send_email", "completed"}
             ]
    end

    test "includes graph-aware step inspection for dependency runs with history enabled" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(DependencyWorkflow, input, repo: Repo)

      assert {:ok, pending_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert Enum.map(pending_run.steps, &{&1.step, &1.status, &1.depends_on}) == [
               {:load_account, :pending, []},
               {:load_invoice, :pending, []},
               {:send_email, :waiting, [:load_account, :load_invoice]}
             ]

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "load_account"}
               })

      assert {:ok, running_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert Enum.map(running_run.steps, &{&1.step, &1.status, &1.depends_on}) == [
               {:load_account, :completed, []},
               {:load_invoice, :pending, []},
               {:send_email, :waiting, [:load_account, :load_invoice]}
             ]

      completed_account_step = Enum.find(running_run.steps, &(&1.step == :load_account))
      assert completed_account_step.output == %{account: %{id: "acct_123", tier: "pro"}}
      assert Enum.map(completed_account_step.attempts, & &1.attempt_number) == [1]
    end

    test "skips stale dependency jobs for steps that were never scheduled" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(DependencyWorkflow, input, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "send_email"}
               })

      assert {:ok, pending_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert pending_run.status == :pending
      assert pending_run.current_step == nil
      assert pending_run.context == %{}

      assert nil ==
               Repo.one(
                 from(step_run in StepRun,
                   where: step_run.run_id == ^run.id and step_run.step == "send_email"
                 )
               )

      assert 2 ==
               SquidMesh.Test.Executor.available_count(run.id)
    end

    test "does not run a dependency join step when one prerequisite fails" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(DependencyFailureWorkflow, input, repo: Repo)

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, failed_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert failed_run.status == :failed
      assert failed_run.current_step == :load_invoice
      assert failed_run.context.account == %{id: "acct_123", tier: "pro"}

      refute Map.has_key?(failed_run.context, :invoice)
      refute Map.has_key?(failed_run.context, :delivery)

      step_runs =
        Repo.all(
          from(step_run in StepRun,
            where: step_run.run_id == ^run.id,
            order_by: [asc: step_run.inserted_at]
          )
        )

      assert Enum.map(step_runs, &{&1.step, &1.status}) == [
               {"load_account", "completed"},
               {"load_invoice", "failed"}
             ]
    end

    test "runs remaining root steps before newly unlocked dependent steps" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(OrderedDependencyWorkflow, input, repo: Repo)
      assert run.current_step == nil

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "load_account"}
               })

      assert {:ok, running_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert running_run.current_step == nil
      refute Map.has_key?(running_run.context, :account_message)

      assert 1 ==
               SquidMesh.Test.Executor.available_count(run.id, "load_invoice")

      assert 0 ==
               SquidMesh.Test.Executor.available_count(run.id, "prepare_account_message")

      assert %{success: 4, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} = SquidMesh.inspect_run(run.id, repo: Repo)

      assert completed_run.status == :completed

      assert completed_run.context.account_message == %{
               account_id: "acct_123",
               status: "prepared"
             }
    end

    test "executes already scheduled dependency steps with their persisted input snapshot" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(InputIsolationWorkflow, input, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "load_account"}
               })

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "load_invoice"}
               })

      assert %{success: success, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert success >= 1

      assert {:ok, completed_run} = SquidMesh.inspect_run(run.id, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.invoice.account_present? == false
    end

    test "supports explicit step input selection and output namespacing" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(ExplicitMappingWorkflow, input, repo: Repo)

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.account == %{id: "acct_123"}

      assert completed_run.context.delivery == %{
               account_id: "acct_123",
               invoice_id: "inv_456"
             }

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.input, &1.output}) == [
               {:load_account, %{account_id: "acct_123"}, %{account: %{id: "acct_123"}}},
               {:record_delivery, %{account: %{id: "acct_123"}, invoice_id: "inv_456"},
                %{delivery: %{account_id: "acct_123", invoice_id: "inv_456"}}}
             ]

      assert Enum.map(completed_run.steps, &{&1.step, &1.status, &1.depends_on}) == [
               {:load_account, :completed, []},
               {:record_delivery, :completed, []}
             ]

      assert Enum.map(completed_run.steps, &{&1.step, &1.input, &1.output}) == [
               {:load_account, %{account_id: "acct_123"}, %{account: %{id: "acct_123"}}},
               {:record_delivery, %{account: %{id: "acct_123"}, invoice_id: "inv_456"},
                %{delivery: %{account_id: "acct_123", invoice_id: "inv_456"}}}
             ]
    end

    test "maps named nested paths into raw Jido action inputs" do
      input = %{account_id: "acct_123", reviewer_id: "user_123"}

      assert {:ok, run} = SquidMesh.start_run(NamedPathMappingWorkflow, input, repo: Repo)

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed

      assert completed_run.context.review == %{
               draft_count: 2,
               reviewer_id: "user_123"
             }

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.input, &1.output}) == [
               {:load_review_context, %{account_id: "acct_123", reviewer_id: "user_123"},
                %{
                  draft: %{drafts: [%{id: "draft_1"}, %{id: "draft_2"}]},
                  review_draft: %{reviewer: %{id: "user_123"}}
                }},
               {:record_review,
                %{drafts: [%{id: "draft_1"}, %{id: "draft_2"}], reviewer: %{id: "user_123"}},
                %{review: %{draft_count: 2, reviewer_id: "user_123"}}}
             ]
    end

    test "maps named nested paths into native Squid Mesh step inputs" do
      input = %{account_id: "acct_123", reviewer_id: "user_123"}

      assert {:ok, run} = SquidMesh.start_run(NativeNamedPathMappingWorkflow, input, repo: Repo)

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.input, &1.output}) == [
               {:load_review_context, %{account_id: "acct_123", reviewer_id: "user_123"},
                %{
                  draft: %{drafts: [%{id: "draft_1"}, %{id: "draft_2"}]},
                  review_draft: %{reviewer: %{id: "user_123"}}
                }},
               {:record_review,
                %{drafts: [%{id: "draft_1"}, %{id: "draft_2"}], reviewer: %{id: "user_123"}},
                %{review: %{draft_count: 2, reviewer_id: "user_123"}}}
             ]
    end

    test "fails before executing a step when a named path input is missing" do
      assert {:ok, run} =
               SquidMesh.start_run(MissingPathMappingWorkflow, %{draft: %{}}, repo: Repo)

      expected_error = %{
        message: "missing mapped input path",
        code: "missing_input_path",
        target: "drafts",
        path: ["draft", "drafts"],
        missing_at: ["draft", "drafts"],
        retryable?: false
      }

      assert {:error,
              {:missing_input_path,
               %{target: :drafts, path: [:draft, :drafts], missing_at: [:draft, :drafts]}}} =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "record_review"}
               })

      assert {:ok, inspected_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert inspected_run.status == :failed
      assert inspected_run.current_step == :record_review
      assert inspected_run.last_error == expected_error

      assert [
               %{step: :record_review, status: :failed, input: %{}, last_error: ^expected_error}
             ] = inspected_run.step_runs
    end

    test "rolls back prepared missing-path step rows when terminal transition fails" do
      assert {:ok, run} =
               SquidMesh.start_run(MissingPathMappingWorkflow, %{draft: %{}}, repo: Repo)

      assert {:ok, definition} = SquidMesh.Workflow.Definition.load(MissingPathMappingWorkflow)

      config = %Config{
        repo: FailedTransitionRepo,
        executor: SquidMesh.Test.Executor,
        stale_step_timeout: :disabled
      }

      assert {:error, _reason} = Preparation.prepare(config, definition, run, :record_review)

      assert {:ok, inspected_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert inspected_run.status == :pending
      assert inspected_run.current_step == :record_review
      assert inspected_run.last_error == nil
      assert inspected_run.step_runs == []
    end

    test "stores a JSON-safe error when named path mapping fails during successor dispatch" do
      assert {:ok, run} =
               SquidMesh.start_run(SuccessMissingPathMappingWorkflow, %{draft: %{}}, repo: Repo)

      assert %{success: 1, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, failed_run} = SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert failed_run.status == :failed
      assert failed_run.current_step == nil

      assert failed_run.last_error == %{
               message: "failed to dispatch workflow step",
               next_steps: ["record_review"],
               dispatch_reason: %{
                 message: "missing mapped input path",
                 code: "missing_input_path",
                 target: "drafts",
                 path: ["draft", "drafts"],
                 missing_at: ["draft", "drafts"],
                 retryable?: false
               }
             }

      assert Enum.map(failed_run.step_runs, &{&1.step, &1.status}) == [
               {:load_review_context, :completed}
             ]
    end

    test "executes native Squid Mesh steps through the Jido-backed runtime" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(NativeStepWorkflow, input, repo: Repo)

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.account == %{id: "acct_123", source_step: "load_account"}

      assert completed_run.context.delivery == %{
               account_id: "acct_123",
               invoice_id: "inv_456",
               attempt: 1
             }

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.input, &1.output}) == [
               {:load_account, %{account_id: "acct_123"},
                %{account: %{id: "acct_123", source_step: "load_account"}}},
               {:record_delivery, %{account: %{id: "acct_123", source_step: "load_account"}},
                %{delivery: %{account_id: "acct_123", invoice_id: "inv_456", attempt: 1}}}
             ]
    end

    test "routes native non-retryable failures without scheduling a retry" do
      input = %{account_id: "acct_123"}

      assert {:ok, run} = SquidMesh.start_run(NativeStepErrorRoutingWorkflow, input, repo: Repo)

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.recovery == %{account_id: "acct_123", reason: "declined"}

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.status, &1.last_error}) == [
               {:charge_card, :failed,
                %{
                  message: "card_declined",
                  code: "card_declined",
                  retryable?: false
                }},
               {:queue_recovery, :completed, nil}
             ]

      failed_step = Enum.find(completed_run.step_runs, &(&1.step == :charge_card))
      assert Enum.map(failed_step.attempts, & &1.attempt_number) == [1]
    end

    test "retries native retryable errors through the workflow retry policy" do
      input = %{account_id: "acct_123"}

      assert {:ok, run} = SquidMesh.start_run(NativeStepRetryWorkflow, input, repo: Repo)

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.gateway_check == %{account_id: "acct_123", status: "ok"}

      assert [%SquidMesh.Steps.Execution{} = step_run] = completed_run.step_runs
      assert step_run.step == :check_gateway
      assert step_run.status == :completed

      assert Enum.map(step_run.attempts, &{&1.attempt_number, &1.status, &1.error}) == [
               {1, :failed,
                %{
                  message: "gateway_timeout",
                  code: "gateway_timeout",
                  retryable?: true
                }},
               {2, :completed, nil}
             ]
    end

    test "uses native success options when dispatching the next step" do
      input = %{account_id: "acct_123"}

      assert {:ok, run} =
               SquidMesh.start_run(NativeStepScheduledSuccessWorkflow, input, repo: Repo)

      assert %{success: 1, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, running_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert running_run.status == :running
      assert running_run.current_step == :record_delivery

      assert %Job{} = scheduled_job = SquidMesh.Test.Executor.scheduled_job(run.id)
      assert DateTime.compare(scheduled_job.scheduled_at, scheduled_job.inserted_at) == :gt
    end

    test "uses native retry_after when scheduling retryable failures" do
      input = %{account_id: "acct_123"}

      assert {:ok, run} = SquidMesh.start_run(NativeStepRetryAfterWorkflow, input, repo: Repo)

      assert %{success: 1, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, retrying_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert retrying_run.status == :retrying
      assert retrying_run.current_step == :check_gateway

      assert retrying_run.last_error == %{
               message: "gateway_timeout",
               code: "gateway_timeout",
               retryable?: true,
               retry_after: 60_000
             }

      assert %Job{} = scheduled_job = SquidMesh.Test.Executor.scheduled_job(run.id)
      assert DateTime.compare(scheduled_job.scheduled_at, scheduled_job.inserted_at) == :gt
    end

    test "routes native input validation failures without scheduling a retry" do
      input = %{account_id: "acct_123"}

      assert {:ok, run} =
               SquidMesh.start_run(NativeStepInputValidationWorkflow, input, repo: Repo)

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.recovery == %{account_id: "acct_123", reason: "validation"}

      assert [failed_step, _recovery_step] = completed_run.step_runs
      assert failed_step.step == :load_invoice
      assert failed_step.status == :failed

      assert failed_step.last_error == %{
               message: "native step input validation failed",
               validation_errors: %{invoice_id: "input field is required"},
               retryable?: false
             }

      assert Enum.map(failed_step.attempts, & &1.attempt_number) == [1]
    end

    test "routes native output validation failures without scheduling a retry" do
      input = %{account_id: "acct_123"}

      assert {:ok, run} =
               SquidMesh.start_run(NativeStepOutputValidationWorkflow, input, repo: Repo)

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.recovery == %{account_id: "acct_123", reason: "validation"}

      assert [failed_step, _recovery_step] = completed_run.step_runs
      assert failed_step.step == :load_account
      assert failed_step.status == :failed

      assert failed_step.last_error == %{
               message: "native step output validation failed",
               validation_errors: %{account: "output field is required"},
               retryable?: false
             }

      assert Enum.map(failed_step.attempts, & &1.attempt_number) == [1]
    end

    test "pauses a run at a built-in pause step until explicitly unblocked" do
      input = %{account_id: "acct_123"}

      assert {:ok, run} = SquidMesh.start_run(PauseWorkflow, input, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      assert {:ok, paused_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert paused_run.status == :paused
      assert paused_run.current_step == :wait_for_approval
      assert paused_run.last_error == nil

      assert 0 ==
               SquidMesh.Test.Executor.available_count(run.id, "record_delivery")

      assert [%SquidMesh.Steps.Execution{} = paused_step] = paused_run.step_runs
      assert paused_step.step == :wait_for_approval
      assert paused_step.status == :running
      assert paused_step.output == nil

      assert [%SquidMesh.Steps.Attempt{attempt_number: 1, status: :running}] =
               paused_step.attempts

      assert {:ok, unblocked_run} = SquidMesh.unblock_run(run.id, repo: Repo)
      assert unblocked_run.status == :running
      assert unblocked_run.current_step == :record_delivery

      assert %{success: success, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert success >= 1

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.current_step == nil

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.status}) == [
               {:wait_for_approval, :completed},
               {:record_delivery, :completed}
             ]
    end

    test "persists pause output mappings after unblock" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseMappedWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      assert {:ok, paused_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert paused_run.status == :paused

      assert {:ok, unblocked_run} = SquidMesh.unblock_run(run.id, repo: Repo)
      assert unblocked_run.status == :running

      assert %{success: success, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert success >= 1

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.approval == %{}
      assert completed_run.context.delivery == %{account_id: "acct_123", approval: %{}}

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.output}) == [
               {:wait_for_approval, %{approval: %{}}},
               {:record_delivery, %{delivery: %{account_id: "acct_123", approval: %{}}}}
             ]
    end

    test "uses persisted pause resume metadata when unblocking" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseMappedWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      persisted_output = %{approval: %{source: "persisted"}}

      assert {1, _rows} =
               Repo.update_all(
                 from(step_run in StepRun,
                   where:
                     step_run.run_id == ^run.id and step_run.step == "wait_for_approval" and
                       step_run.status == "running"
                 ),
                 set: [
                   resume: %{"output" => persisted_output, "target" => "__complete__"}
                 ]
               )

      assert {:ok, unblocked_run} = SquidMesh.unblock_run(run.id, repo: Repo)
      assert unblocked_run.status == :completed
      assert is_nil(unblocked_run.current_step)

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.approval == %{source: "persisted"}

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.status, &1.output}) == [
               {:wait_for_approval, :completed, %{approval: %{source: "persisted"}}}
             ]
    end

    test "routes approved review steps through the explicit approval API" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {:ok, paused_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert paused_run.status == :paused

      assert {:ok, approved_run} =
               SquidMesh.approve_run(
                 run.id,
                 %{actor: "ops_123", comment: "looks good"},
                 repo: Repo
               )

      assert approved_run.status == :running
      assert approved_run.current_step == :record_approval

      assert %{success: success, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert success >= 1

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.approval.decision == "approved"
      assert completed_run.context.approval.actor == "ops_123"
      assert completed_run.context.approval.comment == "looks good"
      assert is_binary(completed_run.context.approval.decided_at)

      assert completed_run.context.approved == %{
               account_id: "acct_123",
               actor: "ops_123",
               decision: "approved"
             }

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.output}) == [
               {:wait_for_review,
                %{
                  approval: %{
                    decision: "approved",
                    actor: "ops_123",
                    comment: "looks good",
                    decided_at: completed_run.context.approval.decided_at
                  }
                }},
               {:record_approval,
                %{
                  approved: %{
                    account_id: "acct_123",
                    actor: "ops_123",
                    decision: "approved"
                  }
                }}
             ]
    end

    test "routes rejected review steps through the explicit rejection API" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {:ok, paused_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert paused_run.status == :paused

      assert {:ok, rejected_run} =
               SquidMesh.reject_run(
                 run.id,
                 %{actor: "ops_456", comment: "insufficient evidence"},
                 repo: Repo
               )

      assert rejected_run.status == :running
      assert rejected_run.current_step == :record_rejection

      assert %{success: success, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert success >= 1

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.approval.decision == "rejected"
      assert completed_run.context.approval.actor == "ops_456"
      assert completed_run.context.approval.comment == "insufficient evidence"
      assert is_binary(completed_run.context.approval.decided_at)

      assert completed_run.context.rejected == %{
               account_id: "acct_123",
               actor: "ops_456",
               decision: "rejected"
             }

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.output}) == [
               {:wait_for_review,
                %{
                  approval: %{
                    decision: "rejected",
                    actor: "ops_456",
                    comment: "insufficient evidence",
                    decided_at: completed_run.context.approval.decided_at
                  }
                }},
               {:record_rejection,
                %{
                  rejected: %{
                    account_id: "acct_123",
                    actor: "ops_456",
                    decision: "rejected"
                  }
                }}
             ]
    end

    test "uses persisted approval resume metadata when approving" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {1, _rows} =
               Repo.update_all(
                 from(step_run in StepRun,
                   where:
                     step_run.run_id == ^run.id and step_run.step == "wait_for_review" and
                       step_run.status == "running"
                 ),
                 set: [
                   resume: %{
                     "kind" => "approval",
                     "ok_target" => "__complete__",
                     "error_target" => "record_rejection",
                     "output_key" => "legacy_approval"
                   }
                 ]
               )

      assert {:ok, approved_run} =
               SquidMesh.approve_run(run.id, %{actor: "ops_123"}, repo: Repo)

      assert approved_run.status == :completed
      assert is_nil(approved_run.current_step)

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context["legacy_approval"].decision == "approved"
      assert completed_run.context["legacy_approval"].actor == "ops_123"

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.status, &1.output}) == [
               {:wait_for_review, :completed,
                %{
                  "legacy_approval" => %{
                    decision: "approved",
                    actor: "ops_123",
                    decided_at: completed_run.context["legacy_approval"].decided_at
                  }
                }}
             ]
    end

    test "falls back to the declared approval output mapping when persisted review metadata is absent" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {1, _rows} =
               Repo.update_all(
                 from(step_run in StepRun,
                   where:
                     step_run.run_id == ^run.id and step_run.step == "wait_for_review" and
                       step_run.status == "running"
                 ),
                 set: [resume: nil]
               )

      assert {:ok, approved_run} =
               SquidMesh.approve_run(run.id, %{actor: "ops_123"}, repo: Repo)

      assert approved_run.status == :running
      assert approved_run.current_step == :record_approval

      assert %{success: success, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert success >= 1

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.approval.decision == "approved"
      assert completed_run.context.approval.actor == "ops_123"
    end

    test "rejects malformed persisted approval output metadata without mutating the paused step" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {1, _rows} =
               Repo.update_all(
                 from(step_run in StepRun,
                   where:
                     step_run.run_id == ^run.id and step_run.step == "wait_for_review" and
                       step_run.status == "running"
                 ),
                 set: [
                   resume: %{
                     "kind" => "approval",
                     "ok_target" => "record_approval",
                     "error_target" => "record_rejection",
                     "output_key" => ["unexpected_key"]
                   }
                 ]
               )

      assert {:error, {:invalid_resume_metadata, :wait_for_review}} =
               SquidMesh.approve_run(run.id, %{actor: "ops_123"}, repo: Repo)

      assert {:ok, paused_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert paused_run.status == :paused
      assert paused_run.current_step == :wait_for_review

      assert Enum.map(paused_run.step_runs, fn step_run ->
               {step_run.step, step_run.status, Enum.map(step_run.attempts, & &1.status)}
             end) == [
               {:wait_for_review, :running, [:running]}
             ]
    end

    test "allows parallel root workers to start from a pending dependency run" do
      :persistent_term.put({ConcurrentDependencyWorkflow, :test_pid}, self())

      on_exit(fn ->
        :persistent_term.erase({ConcurrentDependencyWorkflow, :test_pid})
      end)

      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(ConcurrentDependencyWorkflow, input, repo: Repo)

      account_task =
        Task.async(fn ->
          StepWorker.perform(%Job{
            args: %{"run_id" => run.id, "step" => "load_account"}
          })
        end)

      invoice_task =
        Task.async(fn ->
          StepWorker.perform(%Job{
            args: %{"run_id" => run.id, "step" => "load_invoice"}
          })
        end)

      assert_receive {:concurrent_root_started, :load_account, account_pid}
      assert_receive {:concurrent_root_started, :load_invoice, invoice_pid}

      send(account_pid, :continue)
      send(invoice_pid, :continue)

      assert :ok = Task.await(account_task)
      assert :ok = Task.await(invoice_task)

      assert %{success: success, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert success >= 1

      assert {:ok, completed_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert completed_run.status == :completed
      assert completed_run.context.account == %{id: "acct_123", tier: "pro"}
      assert completed_run.context.invoice == %{id: "inv_456", status: "open"}
      assert completed_run.context.delivery.invoice_id == "inv_456"
    end

    test "does not dispatch dependency join work after a sibling terminally fails the run" do
      :persistent_term.put({ConcurrentDependencyFailureWorkflow, :test_pid}, self())

      on_exit(fn ->
        :persistent_term.erase({ConcurrentDependencyFailureWorkflow, :test_pid})
      end)

      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} =
               SquidMesh.start_run(ConcurrentDependencyFailureWorkflow, input, repo: Repo)

      account_task =
        Task.async(fn ->
          StepWorker.perform(%Job{
            args: %{"run_id" => run.id, "step" => "load_account"}
          })
        end)

      invoice_task =
        Task.async(fn ->
          StepWorker.perform(%Job{
            args: %{"run_id" => run.id, "step" => "load_invoice"}
          })
        end)

      assert_receive {:concurrent_root_started, :load_account, account_pid}
      assert_receive {:concurrent_root_started, :load_invoice, invoice_pid}

      send(invoice_pid, :continue)

      assert :ok = Task.await(invoice_task)

      assert {:ok, failed_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert failed_run.status == :failed
      assert failed_run.current_step == :load_invoice

      send(account_pid, :continue)

      assert :ok = Task.await(account_task)

      assert {:ok, persisted_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert persisted_run.status == :failed
      assert persisted_run.current_step == :load_invoice
      refute Map.has_key?(persisted_run.context, :account)
      refute Map.has_key?(persisted_run.context, :delivery)

      assert 0 ==
               SquidMesh.Test.Executor.available_count(run.id, "send_email")

      step_runs =
        Repo.all(
          from(step_run in StepRun,
            where: step_run.run_id == ^run.id,
            order_by: [asc: step_run.inserted_at]
          )
        )

      assert Enum.map(step_runs, &{&1.step, &1.status}) == [
               {"load_account", "completed"},
               {"load_invoice", "failed"}
             ]
    end

    test "keeps the run retrying when parallel dependency roots fail with retries" do
      :persistent_term.put({ConcurrentRetryWorkflow, :test_pid}, self())

      on_exit(fn ->
        :persistent_term.erase({ConcurrentRetryWorkflow, :test_pid})
      end)

      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(ConcurrentRetryWorkflow, input, repo: Repo)

      account_task =
        Task.async(fn ->
          StepWorker.perform(%Job{
            args: %{"run_id" => run.id, "step" => "load_account"}
          })
        end)

      invoice_task =
        Task.async(fn ->
          StepWorker.perform(%Job{
            args: %{"run_id" => run.id, "step" => "load_invoice"}
          })
        end)

      assert_receive {:concurrent_root_started, :load_account, account_pid}
      assert_receive {:concurrent_root_started, :load_invoice, invoice_pid}

      send(account_pid, :continue)
      send(invoice_pid, :continue)

      assert :ok = Task.await(account_task)
      assert :ok = Task.await(invoice_task)

      assert {:ok, retrying_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert retrying_run.status == :retrying
      assert retrying_run.current_step in [:load_account, :load_invoice]
      assert retrying_run.last_error.code == "gateway_timeout"

      assert 4 ==
               SquidMesh.Test.Executor.available_count(run.id, "load_account") +
                 SquidMesh.Test.Executor.available_count(run.id, "load_invoice")

      step_runs =
        Repo.all(
          from(step_run in StepRun,
            where: step_run.run_id == ^run.id,
            order_by: [asc: step_run.inserted_at]
          )
        )

      assert Enum.sort(Enum.map(step_runs, &{&1.step, &1.status})) == [
               {"load_account", "failed"},
               {"load_invoice", "failed"}
             ]
    end

    test "persists failed step execution and marks the run failed when no retry is declared" do
      assert {:ok, run} =
               SquidMesh.start_run(FailingWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: 1, failure: 0} = SquidMesh.Test.Executor.drain()

      assert {:ok, failed_run} = SquidMesh.inspect_run(run.id, repo: Repo)

      assert failed_run.status == :failed
      assert failed_run.current_step == :check_gateway
      assert failed_run.last_error == %{message: "gateway timeout", code: "gateway_timeout"}

      assert %StepRun{} =
               step_run =
               Repo.one!(
                 from(step_run in StepRun,
                   where: step_run.run_id == ^run.id and step_run.step == "check_gateway"
                 )
               )

      assert step_run.status == "failed"
      assert step_run.last_error == %{"message" => "gateway timeout", "code" => "gateway_timeout"}
      assert AttemptStore.attempt_count(Repo, step_run.id) == 1
    end

    test "wraps local repo-backed step groups in one transaction" do
      assert {:ok, run} =
               SquidMesh.start_run(
                 TransactionalSuccessWorkflow,
                 %{account_id: "acct_123"},
                 repo: Repo
               )

      assert %{success: 1, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} = SquidMesh.inspect_run(run.id, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context.local_transaction == %{status: "committed", rows: 2}
      assert local_transaction_events(run.id) == ["reserved", "captured"]
    end

    test "rolls back local repo-backed step groups when the action fails" do
      assert {:ok, run} =
               SquidMesh.start_run(
                 TransactionalFailureWorkflow,
                 %{account_id: "acct_123"},
                 repo: Repo
               )

      assert %{success: 1, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, failed_run} = SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert failed_run.status == :failed
      assert failed_run.current_step == :write_local_records

      assert failed_run.last_error == %{
               message: "local group failed",
               type: "Jido.Action.Error.ExecutionFailureError"
             }

      assert local_transaction_events(run.id) == []

      assert [%{step: :write_local_records, status: :failed}] = failed_run.step_runs
    end

    test "compensates completed reversible steps in reverse order after terminal failure" do
      :persistent_term.erase({CompensationWorkflow, :events})

      on_exit(fn ->
        :persistent_term.erase({CompensationWorkflow, :events})
      end)

      assert {:ok, run} =
               SquidMesh.start_run(
                 CompensationWorkflow,
                 %{account_id: "acct_123", order_id: "ord_456"},
                 repo: Repo
               )

      assert %{failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, failed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert failed_run.status == :failed
      assert failed_run.current_step == :charge_card
      assert failed_run.last_error == %{message: "card declined", code: "card_declined"}

      assert :persistent_term.get({CompensationWorkflow, :events}) == [
               {:release_inventory, "ord_456"},
               {:release_credit_hold, "acct_123"}
             ]

      assert Enum.map(failed_run.step_runs, &{&1.step, &1.status}) == [
               {:hold_credit, :completed},
               {:reserve_inventory, :completed},
               {:charge_card, :failed}
             ]

      assert %{recovery: %{compensation: %{status: :completed, output: %{released: "credit"}}}} =
               Enum.find(failed_run.step_runs, &(&1.step == :hold_credit))

      assert %{
               recovery: %{
                 compensation: %{status: :completed, output: %{released: "inventory"}}
               }
             } = Enum.find(failed_run.step_runs, &(&1.step == :reserve_inventory))
    end

    test "waits for forward retries to exhaust before compensating completed steps" do
      :persistent_term.erase({CompensationRetryWorkflow, :events})

      on_exit(fn ->
        :persistent_term.erase({CompensationRetryWorkflow, :events})
      end)

      assert {:ok, run} =
               SquidMesh.start_run(
                 CompensationRetryWorkflow,
                 %{account_id: "acct_123"},
                 repo: Repo
               )

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "hold_credit"}
               })

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "charge_card"}
               })

      assert {:ok, retried_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert retried_run.status == :retrying
      assert :persistent_term.get({CompensationRetryWorkflow, :events}, []) == []

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "charge_card"}
               })

      assert {:ok, failed_before_compensation} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert failed_before_compensation.status == :failed
      assert :persistent_term.get({CompensationRetryWorkflow, :events}, []) == []

      assert %Job{} = compensation_job = SquidMesh.Test.Executor.compensation_job(run.id)

      assert :ok = StepWorker.perform(compensation_job)

      assert {:ok, failed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert failed_run.status == :failed

      assert :persistent_term.get({CompensationRetryWorkflow, :events}) == [
               {:release_credit_hold, "acct_123"}
             ]
    end

    test "does not compensate non-failed runs" do
      assert {:ok, run} =
               SquidMesh.start_run(
                 CompensationWorkflow,
                 %{account_id: "acct_123", order_id: "ord_456"},
                 repo: Repo
               )

      assert {:error, {:invalid_compensation_run_status, :pending}} =
               StepExecutor.compensate(run.id, repo: Repo)
    end

    test "persists compensation callback failures for inspection" do
      :persistent_term.erase({CompensationWorkflow, :events})
      :persistent_term.put({CompensationWorkflow, :fail_release_credit?}, true)

      on_exit(fn ->
        :persistent_term.erase({CompensationWorkflow, :events})
        :persistent_term.erase({CompensationWorkflow, :fail_release_credit?})
      end)

      assert {:ok, run} =
               SquidMesh.start_run(
                 CompensationWorkflow,
                 %{account_id: "acct_123", order_id: "ord_456"},
                 repo: Repo
               )

      assert %{failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, failed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert failed_run.status == :failed
      assert failed_run.current_step == :charge_card

      assert :persistent_term.get({CompensationWorkflow, :events}) == [
               {:release_inventory, "ord_456"}
             ]

      assert failed_run.last_error == %{
               message: "workflow step failed and compensation failed",
               failed_step: :charge_card,
               cause: %{message: "card declined", code: "card_declined"},
               compensation_failures: [
                 %{message: "release failed", code: "release_failed", retryable?: false}
               ]
             }

      assert %{
               recovery: %{
                 compensation: %{
                   status: :failed,
                   error: %{message: "release failed", code: "release_failed", retryable?: false}
                 }
               }
             } = Enum.find(failed_run.step_runs, &(&1.step == :hold_credit))

      assert %{
               recovery: %{
                 compensation: %{status: :completed, output: %{released: "inventory"}}
               }
             } = Enum.find(failed_run.step_runs, &(&1.step == :reserve_inventory))
    end

    test "continues to the :error transition when a step fails without retry" do
      assert {:ok, run} =
               SquidMesh.start_run(ErrorRoutingWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.current_step == nil
      assert completed_run.last_error == nil
      assert completed_run.context == %{recovery: %{account_id: "acct_123", status: "queued"}}

      assert Enum.map(completed_run.step_runs, &{&1.step, &1.status}) == [
               {:check_gateway, :failed},
               {:queue_recovery, :completed}
             ]

      assert [
               %{
                 type: :compensation_routed,
                 step: :check_gateway,
                 metadata: %{target: :queue_recovery}
               }
             ] = completed_run.audit_events

      assert [
               %{recovery: %{failure: %{strategy: :compensation, target: :queue_recovery}}},
               _other_step_run
             ] =
               completed_run.step_runs
    end

    test "surfaces undo failure routes distinctly from compensation routes" do
      assert {:ok, run} =
               SquidMesh.start_run(UndoRoutingWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.context == %{undo: %{account_id: "acct_123", status: "released"}}

      assert [
               %{
                 type: :undo_routed,
                 step: :reserve_inventory,
                 metadata: %{target: :release_inventory}
               }
             ] = completed_run.audit_events

      assert [
               %{
                 step: :reserve_inventory,
                 status: :failed,
                 recovery: %{failure: %{strategy: :undo, target: :release_inventory}}
               },
               %{step: :release_inventory, status: :completed}
             ] = completed_run.step_runs
    end

    test "surfaces terminal failure route targets as atoms in inspection history" do
      assert {:ok, run} =
               SquidMesh.start_run(CompleteUndoRoutingWorkflow, %{account_id: "acct_123"},
                 repo: Repo
               )

      assert %{success: 1, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed

      assert [
               %{
                 type: :undo_routed,
                 step: :reserve_inventory,
                 metadata: %{target: :complete}
               }
             ] = completed_run.audit_events

      assert [
               %{
                 step: :reserve_inventory,
                 status: :failed,
                 recovery: %{failure: %{strategy: :undo, target: :complete}}
               }
             ] = completed_run.step_runs
    end

    test "continues to the :error transition only after retries are exhausted" do
      assert {:ok, run} =
               SquidMesh.start_run(ExhaustedRetryWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: 3, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.status == :completed
      assert completed_run.current_step == nil
      assert completed_run.last_error == nil
      assert completed_run.context == %{recovery: %{account_id: "acct_123", status: "queued"}}

      assert [failed_step_run, recovery_step_run] = completed_run.step_runs
      assert {failed_step_run.step, failed_step_run.status} == {:check_gateway, :failed}
      assert {recovery_step_run.step, recovery_step_run.status} == {:queue_recovery, :completed}

      assert Enum.map(failed_step_run.attempts, &{&1.attempt_number, &1.status}) == [
               {1, :failed},
               {2, :failed}
             ]
    end

    test "executes built-in wait and log steps declaratively" do
      assert {:ok, run} =
               SquidMesh.start_run(BuiltInWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: 1, failure: 0} = SquidMesh.Test.Executor.drain()

      assert {:ok, waiting_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert waiting_run.status == :running
      assert waiting_run.current_step == :log_delivery
      assert waiting_run.last_error == nil

      assert %Job{} =
               scheduled_job = SquidMesh.Test.Executor.scheduled_job(run.id, "log_delivery")

      assert DateTime.compare(scheduled_job.scheduled_at, scheduled_job.inserted_at) == :gt

      step_runs =
        Repo.all(
          from(step_run in StepRun,
            where: step_run.run_id == ^run.id,
            order_by: [asc: step_run.inserted_at]
          )
        )

      assert Enum.map(step_runs, &{&1.step, &1.status}) == [
               {"wait_for_settlement", "completed"}
             ]
    end

    test "schedules the next retry attempt through the configured executor when backoff is configured" do
      assert {:ok, run} =
               SquidMesh.start_run(BackoffWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: 1, failure: 0} = SquidMesh.Test.Executor.drain()

      assert {:ok, retried_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert retried_run.status == :retrying
      assert retried_run.current_step == :check_gateway
      assert retried_run.last_error == %{message: "gateway timeout", code: "gateway_timeout"}

      assert %StepRun{} =
               step_run =
               Repo.one!(
                 from(step_run in StepRun,
                   where: step_run.run_id == ^run.id and step_run.step == "check_gateway"
                 )
               )

      assert AttemptStore.attempt_count(Repo, step_run.id) == 1

      assert %Job{} = scheduled_job = SquidMesh.Test.Executor.scheduled_job(run.id)

      assert DateTime.compare(scheduled_job.scheduled_at, scheduled_job.inserted_at) == :gt
    end

    test "does not let Jido retries consume the workflow retry boundary" do
      :persistent_term.erase({RetrySurfaceWorkflow.FailOnce, :attempts})

      on_exit(fn ->
        :persistent_term.erase({RetrySurfaceWorkflow.FailOnce, :attempts})
      end)

      assert {:ok, run} =
               SquidMesh.start_run(RetrySurfaceWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: 1, failure: 0} = SquidMesh.Test.Executor.drain()

      assert {:ok, retried_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert retried_run.status == :retrying
      assert retried_run.current_step == :check_gateway
      assert retried_run.last_error == %{message: "gateway timeout", code: "gateway_timeout"}

      assert %StepRun{} =
               step_run =
               Repo.one!(
                 from(step_run in StepRun,
                   where: step_run.run_id == ^run.id and step_run.step == "check_gateway"
                 )
               )

      assert step_run.status == "failed"
      assert AttemptStore.attempt_count(Repo, step_run.id) == 1
    end

    test "ignores stale step jobs after the run advances to the next step" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(SuccessfulWorkflow, input, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "load_invoice"}
               })

      assert {:ok, running_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert running_run.status == :running
      assert running_run.current_step == :send_email

      load_invoice_step_run =
        Repo.one!(
          from(step_run in StepRun,
            where: step_run.run_id == ^run.id and step_run.step == "load_invoice"
          )
        )

      assert AttemptStore.attempt_count(Repo, load_invoice_step_run.id) == 1

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "load_invoice"}
               })

      assert AttemptStore.attempt_count(Repo, load_invoice_step_run.id) == 1

      assert 1 ==
               SquidMesh.Test.Executor.available_count(run.id, "send_email")
    end

    test "does not re-execute a step that is already marked running" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(SuccessfulWorkflow, input, repo: Repo)
      assert {:ok, running_run} = SquidMesh.Runs.Store.transition_run(Repo, run.id, :running)

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, running_run.id, :load_invoice, input)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "load_invoice"}
               })

      assert AttemptStore.attempt_count(Repo, step_run.id) == 0

      assert %StepRun{status: "running"} =
               Repo.get!(StepRun, step_run.id)
    end

    test "marks the run failed when dispatching the next step fails after a successful step" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, SuccessfulWorkflow, input)
      assert :ok = StepExecutor.execute(run.id, nil, repo: Repo, executor: MissingExecutor)

      assert {:ok, failed_run} = SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert failed_run.status == :failed
      assert failed_run.current_step == :send_email
      assert failed_run.context.account == %{id: "acct_123"}
      assert failed_run.context.invoice == %{id: "inv_456", status: "open"}
      assert failed_run.last_error.message == "failed to dispatch workflow step"
      assert failed_run.last_error.next_step == :send_email
      assert failed_run.last_error.cause == "executor_unavailable"

      assert [%SquidMesh.Steps.Execution{} = step_run] = failed_run.step_runs
      assert step_run.step == :load_invoice
      assert step_run.status == :completed

      assert Enum.map(step_run.attempts, fn attempt ->
               {attempt.attempt_number, attempt.status}
             end) == [{1, :completed}]
    end

    test "preserves sibling dependency context when join dispatch fails after parallel success" do
      :persistent_term.put({ConcurrentDependencyWorkflow, :test_pid}, self())

      on_exit(fn ->
        :persistent_term.erase({ConcurrentDependencyWorkflow, :test_pid})
      end)

      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.start_run(ConcurrentDependencyWorkflow, input, repo: Repo)

      account_task =
        Task.async(fn ->
          StepExecutor.execute(run.id, :load_account, repo: Repo, executor: MissingExecutor)
        end)

      invoice_task =
        Task.async(fn ->
          StepExecutor.execute(run.id, :load_invoice, repo: Repo, executor: MissingExecutor)
        end)

      assert_receive {:concurrent_root_started, :load_account, account_pid}
      assert_receive {:concurrent_root_started, :load_invoice, invoice_pid}

      send(invoice_pid, :continue)

      assert :ok = Task.await(invoice_task)

      assert {:ok, invoice_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert invoice_run.status == :running
      assert invoice_run.context.invoice == %{id: "inv_456", status: "open"}
      refute Map.has_key?(invoice_run.context, :account)

      send(account_pid, :continue)

      assert :ok = Task.await(account_task)

      assert {:ok, failed_run} = SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert failed_run.status == :failed
      assert failed_run.current_step == nil
      assert failed_run.context.account == %{id: "acct_123", tier: "pro"}
      assert failed_run.context.invoice == %{id: "inv_456", status: "open"}
      refute Map.has_key?(failed_run.context, :delivery)
      assert failed_run.last_error.message == "failed to dispatch workflow step"
      assert failed_run.last_error.next_steps == ["send_email"]
    end

    test "marks the run failed if dependency resolution cannot find a runnable next step" do
      config = Config.load!(repo: Repo)
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, DependencyWorkflow, input)

      assert {:ok, completed_root, :execute} =
               Steps.Store.begin_step(Repo, run.id, :load_account, input)

      assert {:ok, _completed_root} =
               Steps.Store.complete_step(Repo, completed_root.id, %{
                 account: %{id: "acct_123", tier: "pro"}
               })

      assert {:ok, prepared_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :load_invoice,
                 context: %{account: %{id: "acct_123", tier: "pro"}}
               })

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, prepared_run.id, :load_invoice, input)

      assert {:ok, attempt} = AttemptStore.begin_attempt(Repo, step_run.id)

      invalid_definition =
        Map.update!(DependencyWorkflow.workflow_definition(), :steps, fn steps ->
          Enum.map(steps, fn
            %{name: :send_email} = step ->
              %{step | opts: [after: [:missing_dependency]]}

            step ->
              step
          end)
        end)

      assert :ok =
               Outcome.apply_execution_result(
                 {:ok, %{invoice: %{id: "inv_456", status: "open"}}, []},
                 %{
                   config: config,
                   definition: invalid_definition,
                   run: prepared_run,
                   step_name: :load_invoice,
                   step_run_id: step_run.id,
                   attempt_id: attempt.id,
                   attempt_number: attempt.attempt_number,
                   started_at: System.monotonic_time()
                 }
               )

      assert {:ok, failed_run} = SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)
      assert failed_run.status == :failed
      assert failed_run.current_step == :load_invoice
      assert failed_run.context.invoice == %{id: "inv_456", status: "open"}

      assert failed_run.last_error.message ==
               "workflow step completed but no runnable next step was found"

      assert failed_run.last_error.failed_step == :load_invoice
      assert failed_run.last_error.pending_steps == [:send_email]
    end

    test "marks the run failed if dependency resolution raises after the step succeeds" do
      config = Config.load!(repo: Repo)
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, DependencyWorkflow, input)

      assert {:ok, completed_root, :execute} =
               Steps.Store.begin_step(Repo, run.id, :load_account, input)

      assert {:ok, _completed_root} =
               Steps.Store.complete_step(Repo, completed_root.id, %{
                 account: %{id: "acct_123", tier: "pro"}
               })

      assert {:ok, prepared_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :load_invoice,
                 context: %{account: %{id: "acct_123", tier: "pro"}}
               })

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, prepared_run.id, :load_invoice, input)

      assert {:ok, attempt} = AttemptStore.begin_attempt(Repo, step_run.id)

      invalid_definition =
        Map.update!(DependencyWorkflow.workflow_definition(), :steps, fn steps ->
          Enum.map(steps, fn
            %{name: :load_account} = step ->
              %{step | opts: [after: [:send_email]]}

            %{name: :send_email} = step ->
              %{step | opts: [after: [:load_account, :load_invoice]]}

            step ->
              step
          end)
        end)

      assert :ok =
               Outcome.apply_execution_result(
                 {:ok, %{invoice: %{id: "inv_456", status: "open"}}, []},
                 %{
                   config: config,
                   definition: invalid_definition,
                   run: prepared_run,
                   step_name: :load_invoice,
                   step_run_id: step_run.id,
                   attempt_id: attempt.id,
                   attempt_number: attempt.attempt_number,
                   started_at: System.monotonic_time()
                 }
               )

      assert {:ok, failed_run} = SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)
      assert failed_run.status == :failed
      assert failed_run.current_step == :load_invoice
      assert failed_run.context.invoice == %{id: "inv_456", status: "open"}

      assert failed_run.last_error.message ==
               "workflow step completed but next step resolution failed"

      assert failed_run.last_error.failed_step == :load_invoice

      assert failed_run.last_error.cause == %{
               reason: "invalid_dependency_graph",
               message: "workflow dependency graph must be acyclic"
             }
    end

    test "marks the run failed when scheduling a retry fails" do
      assert {:ok, run} =
               SquidMesh.Runs.Store.create_run(Repo, BackoffWorkflow, %{account_id: "acct_123"})

      assert :ok = StepExecutor.execute(run.id, nil, repo: Repo, executor: MissingExecutor)

      assert {:ok, failed_run} = SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert failed_run.status == :failed
      assert failed_run.current_step == :check_gateway
      assert failed_run.last_error.message == "failed to dispatch workflow step"
      assert failed_run.last_error.failed_step == :check_gateway
      assert failed_run.last_error.cause == %{message: "gateway timeout", code: "gateway_timeout"}

      assert [%SquidMesh.Steps.Execution{} = step_run] = failed_run.step_runs
      assert step_run.step == :check_gateway
      assert step_run.status == :failed

      assert Enum.map(step_run.attempts, fn attempt ->
               {attempt.attempt_number, attempt.status}
             end) == [{1, :failed}]
    end

    test "reconciles completed step history when redelivery finds run state behind it" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, SuccessfulWorkflow, input)

      assert {:ok, running_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :load_invoice
               })

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, running_run.id, :load_invoice, input)

      assert {:ok, attempt} = AttemptStore.begin_attempt(Repo, step_run.id)
      assert {:ok, _attempt} = AttemptStore.complete_attempt(Repo, attempt.id)

      assert {:ok, _step_run} =
               Steps.Store.complete_step(Repo, step_run.id, %{
                 account: %{id: "acct_123"},
                 invoice: %{id: "inv_456", status: "open"}
               })

      assert :ok = StepExecutor.execute(run.id, :load_invoice, repo: Repo)

      assert {:ok, reconciled_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert reconciled_run.status == :running
      assert reconciled_run.current_step == :send_email
      assert reconciled_run.context.account == %{id: "acct_123"}
      assert reconciled_run.context.invoice == %{id: "inv_456", status: "open"}

      assert AttemptStore.attempt_count(Repo, step_run.id) == 1

      assert_enqueued(
        worker: SquidMesh.Test.StepWorker,
        queue: "squid_mesh",
        args: %{"run_id" => run.id, "step" => "send_email"}
      )
    end

    test "reclaims stale running attempts before executing a redelivered step" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, SuccessfulWorkflow, input)

      assert {:ok, running_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :load_invoice
               })

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, running_run.id, :load_invoice, input)

      assert {:ok, first_attempt} = AttemptStore.begin_attempt(Repo, step_run.id)
      first_attempt_id = first_attempt.id

      stale_time =
        DateTime.utc_now()
        |> DateTime.add(-120, :second)
        |> DateTime.truncate(:microsecond)

      Repo.update_all(
        from(stale_step in StepRun, where: stale_step.id == ^step_run.id),
        set: [updated_at: stale_time]
      )

      assert :ok =
               StepExecutor.execute(run.id, :load_invoice,
                 repo: Repo,
                 stale_step_timeout: 0
               )

      assert {:ok, progressed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert progressed_run.status == :running
      assert progressed_run.current_step == :send_email
      assert progressed_run.context.invoice == %{id: "inv_456", status: "open"}

      assert [%SquidMesh.Steps.Execution{} = completed_step] = progressed_run.step_runs
      assert completed_step.status == :completed

      assert [
               %{id: ^first_attempt_id, attempt_number: 1, status: :failed},
               %{attempt_number: 2, status: :completed}
             ] = completed_step.attempts
    end

    test "rejects stale successful completion when a newer attempt is running" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}
      assert {:ok, config} = Config.load(repo: Repo)
      assert {:ok, definition} = SquidMesh.Workflow.Definition.load(SuccessfulWorkflow)
      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, SuccessfulWorkflow, input)

      assert {:ok, running_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :load_invoice
               })

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, running_run.id, :load_invoice, input)

      assert {:ok, stale_attempt} = AttemptStore.begin_attempt(Repo, step_run.id)
      stale_attempt_id = stale_attempt.id
      assert {:ok, current_attempt} = AttemptStore.begin_attempt(Repo, step_run.id)

      assert {:error, {:stale_attempt, "running"}} =
               Outcome.apply_execution_result(
                 {:ok,
                  %{
                    account: %{id: "acct_123"},
                    invoice: %{id: "inv_456", status: "open"}
                  }, []},
                 %{
                   config: config,
                   definition: definition,
                   run: running_run,
                   step_name: :load_invoice,
                   step_run_id: step_run.id,
                   attempt_id: stale_attempt.id,
                   attempt_number: stale_attempt.attempt_number,
                   started_at: System.monotonic_time()
                 }
               )

      assert {:ok, inspected_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert inspected_run.status == :running
      assert inspected_run.current_step == :load_invoice
      refute Map.has_key?(inspected_run.context, :invoice)

      assert [%SquidMesh.Steps.Execution{} = running_step] = inspected_run.step_runs
      assert running_step.status == :running

      assert [
               %{id: ^stale_attempt_id, attempt_number: 1, status: :running},
               %{id: current_attempt_id, attempt_number: 2, status: :running}
             ] = running_step.attempts

      assert current_attempt_id == current_attempt.id
    end

    test "rejects stale failure when a newer attempt is running" do
      input = %{account_id: "acct_123"}
      assert {:ok, config} = Config.load(repo: Repo)
      assert {:ok, definition} = SquidMesh.Workflow.Definition.load(BackoffWorkflow)
      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, BackoffWorkflow, input)

      assert {:ok, running_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :check_gateway
               })

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, running_run.id, :check_gateway, input)

      assert {:ok, stale_attempt} = AttemptStore.begin_attempt(Repo, step_run.id)
      stale_attempt_id = stale_attempt.id
      assert {:ok, current_attempt} = AttemptStore.begin_attempt(Repo, step_run.id)

      assert {:error, {:stale_attempt, "running"}} =
               Outcome.apply_execution_result(
                 {:error, %{message: "gateway timeout", code: "gateway_timeout"}},
                 %{
                   config: config,
                   definition: definition,
                   run: running_run,
                   step_name: :check_gateway,
                   step_run_id: step_run.id,
                   attempt_id: stale_attempt.id,
                   attempt_number: stale_attempt.attempt_number,
                   started_at: System.monotonic_time()
                 }
               )

      assert {:ok, inspected_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert inspected_run.status == :running
      assert inspected_run.current_step == :check_gateway
      assert inspected_run.last_error == nil

      assert [%SquidMesh.Steps.Execution{} = running_step] = inspected_run.step_runs
      assert running_step.status == :running
      assert running_step.last_error == nil

      assert [
               %{id: ^stale_attempt_id, attempt_number: 1, status: :running},
               %{id: current_attempt_id, attempt_number: 2, status: :running}
             ] = running_step.attempts

      assert current_attempt_id == current_attempt.id
    end

    test "rejects stale pause completion after a newer attempt takes over" do
      assert {:ok, config} = Config.load(repo: Repo)
      assert {:ok, definition} = SquidMesh.Workflow.Definition.load(PauseWorkflow)

      assert {:ok, run} =
               SquidMesh.Runs.Store.create_run(Repo, PauseWorkflow, %{account_id: "acct_123"})

      assert {:ok, running_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :wait_for_approval
               })

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, running_run.id, :wait_for_approval, %{
                 account_id: "acct_123"
               })

      assert {:ok, stale_attempt} = AttemptStore.begin_attempt(Repo, step_run.id)
      stale_attempt_id = stale_attempt.id

      stale_time =
        DateTime.utc_now()
        |> DateTime.add(-120, :second)
        |> DateTime.truncate(:microsecond)

      Repo.update_all(
        from(stale_step in StepRun, where: stale_step.id == ^step_run.id),
        set: [updated_at: stale_time]
      )

      assert {:ok, :reclaimed} =
               SquidMesh.Runtime.StepRecovery.reclaim_stale_running_step(Repo, step_run, 0)

      assert {:ok, reclaimed_step, :execute} =
               Steps.Store.begin_step(Repo, running_run.id, :wait_for_approval, %{
                 account_id: "acct_123"
               })

      assert {:ok, current_attempt} = AttemptStore.begin_attempt(Repo, reclaimed_step.id)

      assert {:error, {:stale_attempt, "failed"}} =
               Outcome.apply_execution_result(
                 {:ok, %{}, [pause: true]},
                 %{
                   config: config,
                   definition: definition,
                   run: running_run,
                   step_name: :wait_for_approval,
                   step_run_id: step_run.id,
                   attempt_id: stale_attempt.id,
                   attempt_number: stale_attempt.attempt_number,
                   started_at: System.monotonic_time()
                 }
               )

      assert {:ok, inspected_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert inspected_run.status == :running
      assert inspected_run.current_step == :wait_for_approval

      assert [%SquidMesh.Steps.Execution{} = paused_step] = inspected_run.step_runs
      assert paused_step.status == :running

      assert Repo.get!(StepRun, step_run.id).resume == nil

      assert [
               %{id: ^stale_attempt_id, attempt_number: 1, status: :failed},
               %{id: current_attempt_id, attempt_number: 2, status: :running}
             ] = paused_step.attempts

      assert current_attempt_id == current_attempt.id
    end

    test "valid paused runs keep the manual step attempt open for unblock" do
      assert {:ok, run} =
               SquidMesh.start_run(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 repo: Repo
               )

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      assert {:ok, paused_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert paused_run.status == :paused
      assert paused_run.current_step == :wait_for_approval

      assert [%SquidMesh.Steps.Execution{} = manual_step] = paused_run.step_runs
      assert manual_step.step == :wait_for_approval
      assert manual_step.status == :running
      assert [%{attempt_number: 1, status: :running}] = manual_step.attempts
    end

    test "rejects stale approval pause after a newer attempt takes over" do
      assert {:ok, config} = Config.load(repo: Repo)
      assert {:ok, definition} = SquidMesh.Workflow.Definition.load(ApprovalWorkflow)

      assert {:ok, run} =
               SquidMesh.Runs.Store.create_run(Repo, ApprovalWorkflow, %{account_id: "acct_123"})

      assert {:ok, running_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :wait_for_review
               })

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, running_run.id, :wait_for_review, %{
                 account_id: "acct_123"
               })

      assert {:ok, stale_attempt} = AttemptStore.begin_attempt(Repo, step_run.id)
      stale_attempt_id = stale_attempt.id

      stale_time =
        DateTime.utc_now()
        |> DateTime.add(-120, :second)
        |> DateTime.truncate(:microsecond)

      Repo.update_all(
        from(stale_step in StepRun, where: stale_step.id == ^step_run.id),
        set: [updated_at: stale_time]
      )

      assert {:ok, :reclaimed} =
               SquidMesh.Runtime.StepRecovery.reclaim_stale_running_step(Repo, step_run, 0)

      assert {:ok, reclaimed_step, :execute} =
               Steps.Store.begin_step(Repo, running_run.id, :wait_for_review, %{
                 account_id: "acct_123"
               })

      assert {:ok, current_attempt} = AttemptStore.begin_attempt(Repo, reclaimed_step.id)

      assert {:error, {:stale_attempt, "failed"}} =
               Outcome.apply_execution_result(
                 {:ok, %{}, [pause: true]},
                 %{
                   config: config,
                   definition: definition,
                   run: running_run,
                   step_name: :wait_for_review,
                   step_run_id: step_run.id,
                   attempt_id: stale_attempt.id,
                   attempt_number: stale_attempt.attempt_number,
                   started_at: System.monotonic_time()
                 }
               )

      assert {:ok, inspected_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert inspected_run.status == :running
      assert inspected_run.current_step == :wait_for_review

      assert [%SquidMesh.Steps.Execution{} = approval_step] = inspected_run.step_runs
      assert approval_step.status == :running
      assert Repo.get!(StepRun, step_run.id).resume == nil

      assert [
               %{id: ^stale_attempt_id, attempt_number: 1, status: :failed},
               %{id: current_attempt_id, attempt_number: 2, status: :running}
             ] = approval_step.attempts

      assert current_attempt_id == current_attempt.id
    end

    test "skips stale running attempts by default" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, SuccessfulWorkflow, input)

      assert {:ok, running_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :load_invoice
               })

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, running_run.id, :load_invoice, input)

      assert {:ok, first_attempt} = AttemptStore.begin_attempt(Repo, step_run.id)

      stale_time =
        DateTime.utc_now()
        |> DateTime.add(-120, :second)
        |> DateTime.truncate(:microsecond)

      Repo.update_all(
        from(stale_step in StepRun, where: stale_step.id == ^step_run.id),
        set: [updated_at: stale_time]
      )

      assert :ok = StepExecutor.execute(run.id, :load_invoice, repo: Repo)

      assert {:ok, inspected_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert inspected_run.status == :running
      assert inspected_run.current_step == :load_invoice

      assert [%SquidMesh.Steps.Execution{} = running_step] = inspected_run.step_runs
      assert running_step.status == :running
      assert [%{id: attempt_id, attempt_number: 1, status: :running}] = running_step.attempts
      assert attempt_id == first_attempt.id
    end

    test "does not partially schedule fan-out dispatch when a later step is invalid" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, config} = Config.load(repo: Repo)
      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, DependencyWorkflow, input)

      assert {:error, {:unknown_step, :missing_step}} =
               Dispatcher.dispatch_steps(
                 config,
                 run,
                 [:load_account, :missing_step],
                 schedule_pending: true
               )

      refute Repo.exists?(from(step_run in StepRun, where: step_run.run_id == ^run.id))

      refute Enum.any?(SquidMesh.Test.Executor.jobs(), &(&1.args["run_id"] == run.id))
    end

    test "rolls back pending fan-out rows when direct dispatch fails after scheduling" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, config} = Config.load(repo: Repo, executor: MissingExecutor)
      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, DependencyWorkflow, input)

      assert {:error, _reason} =
               Dispatcher.dispatch_steps(config, run, [:load_account, :load_invoice],
                 schedule_pending: true
               )

      refute Repo.exists?(from(step_run in StepRun, where: step_run.run_id == ^run.id))
    end

    test "keeps existing pending rows when dispatch fails without scheduling" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, config} = Config.load(repo: Repo, executor: MissingExecutor)
      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, DependencyWorkflow, input)

      assert {:ok, [:load_account]} =
               Steps.Store.schedule_steps(Repo, run.id, [
                 {:load_account, %{account_id: "acct_123"}}
               ])

      assert {:error, _reason} = Dispatcher.dispatch_steps(config, run, [:load_account])

      assert %StepRun{status: "pending"} =
               Repo.one(
                 from(step_run in StepRun,
                   where: step_run.run_id == ^run.id and step_run.step == "load_account"
                 )
               )
    end

    test "converges cancelling runs to cancelled even when a stale scheduled step arrives" do
      assert {:ok, run} =
               SquidMesh.start_run(BuiltInWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: 1, failure: 0} = SquidMesh.Test.Executor.drain()

      assert {:ok, cancelling_run} = SquidMesh.cancel_run(run.id, repo: Repo)
      assert cancelling_run.status == :cancelling
      assert cancelling_run.current_step == nil

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "log_delivery"}
               })

      assert {:ok, cancelled_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert cancelled_run.status == :cancelled
      assert cancelled_run.current_step == nil
    end

    test "does not claim a step when preparation observes a stale running run after cancellation" do
      input = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, run} = SquidMesh.Runs.Store.create_run(Repo, SuccessfulWorkflow, input)

      assert {:ok, stale_running_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :load_invoice
               })

      assert {:ok, cancelling_run} = SquidMesh.cancel_run(run.id, repo: Repo)
      assert cancelling_run.status == :cancelling

      assert {:ok, config} = Config.load(repo: Repo)
      assert {:ok, definition} = SquidMesh.Workflow.Definition.load(SuccessfulWorkflow)

      assert {:cancel, locked_run} =
               Preparation.prepare(config, definition, stale_running_run, :load_invoice)

      assert locked_run.status == :cancelling

      refute Repo.one(
               from(step_run in StepRun,
                 where: step_run.run_id == ^run.id and step_run.step == "load_invoice"
               )
             )
    end

    test "converges to cancelled when a post-wait step finishes after cancellation is requested" do
      :persistent_term.put({CancellationCompletionWorkflow.RecordDelivery, :test_pid}, self())

      on_exit(fn ->
        :persistent_term.erase({CancellationCompletionWorkflow.RecordDelivery, :test_pid})
      end)

      assert {:ok, run} =
               SquidMesh.start_run(
                 CancellationCompletionWorkflow,
                 %{account_id: "acct_123"},
                 repo: Repo
               )

      assert %{success: 1, failure: 0} = SquidMesh.Test.Executor.drain()

      assert {:ok, ready_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert ready_run.status == :running
      assert ready_run.current_step == :record_delivery

      task =
        Task.async(fn ->
          StepWorker.perform(%Job{
            args: %{"run_id" => run.id, "step" => "record_delivery"}
          })
        end)

      assert_receive {:record_delivery_started, delivery_pid, "acct_123"}

      assert {:ok, cancelling_run} = SquidMesh.cancel_run(run.id, repo: Repo)
      assert cancelling_run.status == :cancelling

      send(delivery_pid, :continue)

      assert :ok = Task.await(task)

      assert {:ok, cancelled_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert cancelled_run.status == :cancelled
      assert cancelled_run.current_step == nil
    end

    test "finalizes pause step history when cancellation wins before pause progression" do
      assert {:ok, config} = Config.load(repo: Repo)
      assert {:ok, definition} = SquidMesh.Workflow.Definition.load(PauseWorkflow)

      assert {:ok, run} =
               SquidMesh.start_run(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 repo: Repo
               )

      assert {:ok, running_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :wait_for_approval
               })

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, run.id, :wait_for_approval, %{
                 account_id: "acct_123"
               })

      assert {:ok, attempt} = AttemptStore.begin_attempt(Repo, step_run.id)

      assert {:ok, cancelling_run} = SquidMesh.cancel_run(run.id, repo: Repo)
      assert cancelling_run.status == :cancelling

      started_at = System.monotonic_time()

      assert :ok =
               Outcome.apply_execution_result(
                 {:ok, %{}, [pause: true]},
                 %{
                   config: config,
                   definition: definition,
                   run: running_run,
                   step_name: :wait_for_approval,
                   step_run_id: step_run.id,
                   attempt_id: attempt.id,
                   attempt_number: attempt.attempt_number,
                   started_at: started_at
                 }
               )

      assert {:ok, cancelled_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert cancelled_run.status == :cancelled
      assert cancelled_run.current_step == nil

      assert [%SquidMesh.Steps.Execution{} = paused_step] = cancelled_run.step_runs
      assert paused_step.step == :wait_for_approval
      assert paused_step.status == :failed
      assert paused_step.output == nil

      assert paused_step.last_error == %{
               message: "run cancelled while paused",
               reason: "cancelled"
             }

      assert Enum.map(paused_step.attempts, &{&1.status, &1.error}) == [
               {:failed, %{message: "run cancelled while paused", reason: "cancelled"}}
             ]
    end

    test "finalizes pause step history when pause progression sees an already-cancelled run" do
      assert {:ok, config} = Config.load(repo: Repo)
      assert {:ok, definition} = SquidMesh.Workflow.Definition.load(PauseWorkflow)

      assert {:ok, run} =
               SquidMesh.start_run(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 repo: Repo
               )

      assert {:ok, running_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :wait_for_approval
               })

      assert {:ok, step_run, :execute} =
               Steps.Store.begin_step(Repo, run.id, :wait_for_approval, %{
                 account_id: "acct_123"
               })

      assert {:ok, attempt} = AttemptStore.begin_attempt(Repo, step_run.id)

      assert {:ok, cancelling_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :cancelling)

      assert cancelling_run.status == :cancelling

      assert {:ok, cancelled_run} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :cancelled, %{current_step: nil})

      assert cancelled_run.status == :cancelled

      assert :ok =
               Outcome.apply_execution_result(
                 {:ok, %{}, [pause: true]},
                 %{
                   config: config,
                   definition: definition,
                   run: running_run,
                   step_name: :wait_for_approval,
                   step_run_id: step_run.id,
                   attempt_id: attempt.id,
                   attempt_number: attempt.attempt_number,
                   started_at: System.monotonic_time()
                 }
               )

      assert {:ok, current_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert current_run.status == :cancelled
      assert current_run.current_step == nil

      assert [%SquidMesh.Steps.Execution{} = paused_step] = current_run.step_runs
      assert paused_step.step == :wait_for_approval
      assert paused_step.status == :failed

      assert paused_step.last_error == %{
               message: "run cancelled while paused",
               reason: "cancelled"
             }

      assert Enum.map(paused_step.attempts, &{&1.status, &1.error}) == [
               {:failed, %{message: "run cancelled while paused", reason: "cancelled"}}
             ]
    end
  end

  defp local_transaction_events(run_id) do
    dumped_run_id = Ecto.UUID.dump!(run_id)

    Repo.all(
      from(event in "transactional_events",
        where: event.run_id == ^dumped_run_id,
        order_by: [asc: event.id],
        select: event.event
      )
    )
  end

  defmodule DependencyWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, DependencyWorkflow.LoadAccount
      step :load_invoice, DependencyWorkflow.LoadInvoice
      step :send_email, DependencyWorkflow.SendEmail, after: [:load_account, :load_invoice]
    end
  end

  defmodule DependencyFailureWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, DependencyWorkflow.LoadAccount
      step :load_invoice, DependencyFailureWorkflow.LoadInvoice
      step :send_email, DependencyWorkflow.SendEmail, after: [:load_account, :load_invoice]
    end
  end

  defmodule DependencyWorkflow.LoadAccount do
    use Jido.Action,
      name: "load_account",
      description: "Loads account details",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{account: %{id: account_id, tier: "pro"}}}
    end
  end

  defmodule DependencyWorkflow.LoadInvoice do
    use Jido.Action,
      name: "load_invoice",
      description: "Loads invoice details",
      schema: [
        invoice_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{invoice_id: invoice_id}, _context) do
      {:ok, %{invoice: %{id: invoice_id, status: "open"}}}
    end
  end

  defmodule DependencyWorkflow.SendEmail do
    use Jido.Action,
      name: "send_email",
      description: "Sends a recovery email after both inputs are ready",
      schema: [
        account: [type: :map, required: true],
        invoice: [type: :map, required: true]
      ]

    @impl Jido.Action
    def run(%{account: account, invoice: invoice}, _context) do
      {:ok,
       %{
         delivery: %{
           account_id: account.id,
           invoice_id: invoice.id,
           channel: "email"
         }
       }}
    end
  end

  defmodule OrderedDependencyWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, DependencyWorkflow.LoadAccount

      step :prepare_account_message, OrderedDependencyWorkflow.PrepareAccountMessage,
        after: [:load_account]

      step :load_invoice, DependencyWorkflow.LoadInvoice

      step :send_email, OrderedDependencyWorkflow.SendEmail,
        after: [:prepare_account_message, :load_invoice]
    end
  end

  defmodule OrderedDependencyWorkflow.PrepareAccountMessage do
    use Jido.Action,
      name: "prepare_account_message",
      description: "Builds intermediate account context",
      schema: [
        account: [type: :map, required: true]
      ]

    @impl Jido.Action
    def run(%{account: account}, _context) do
      {:ok, %{account_message: %{account_id: account.id, status: "prepared"}}}
    end
  end

  defmodule OrderedDependencyWorkflow.SendEmail do
    use Jido.Action,
      name: "send_email",
      description: "Sends a recovery email after ordered dependency execution",
      schema: [
        account_message: [type: :map, required: true],
        invoice: [type: :map, required: true]
      ]

    @impl Jido.Action
    def run(%{account_message: account_message, invoice: invoice}, _context) do
      {:ok,
       %{
         delivery: %{
           account_id: account_message.account_id,
           invoice_id: invoice.id,
           channel: "email"
         }
       }}
    end
  end

  defmodule InputIsolationWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, DependencyWorkflow.LoadAccount
      step :load_invoice, InputIsolationWorkflow.LoadInvoice

      step :complete_run, :log,
        message: "dependency roots completed",
        after: [:load_account, :load_invoice]
    end
  end

  defmodule InputIsolationWorkflow.LoadInvoice do
    use Jido.Action,
      name: "load_invoice",
      description: "Checks whether sibling context leaked into a scheduled root step",
      schema: [
        invoice_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{invoice_id: invoice_id} = input, _context) do
      {:ok,
       %{
         invoice: %{
           id: invoice_id,
           account_present?: Map.has_key?(input, :account)
         }
       }}
    end
  end

  defmodule ConcurrentDependencyWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, ConcurrentDependencyWorkflow.LoadAccount
      step :load_invoice, ConcurrentDependencyWorkflow.LoadInvoice
      step :send_email, DependencyWorkflow.SendEmail, after: [:load_account, :load_invoice]
    end
  end

  defmodule ConcurrentDependencyWorkflow.LoadAccount do
    use Jido.Action,
      name: "load_account",
      description: "Blocks until the test releases the account root",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      notify_test(:load_account)
      await_continue()
      {:ok, %{account: %{id: account_id, tier: "pro"}}}
    end

    defp notify_test(step_name) do
      case :persistent_term.get({ConcurrentDependencyWorkflow, :test_pid}, nil) do
        pid when is_pid(pid) -> send(pid, {:concurrent_root_started, step_name, self()})
        _other -> :ok
      end
    end

    defp await_continue do
      receive do
        :continue -> :ok
      after
        1_000 -> raise "timed out waiting for concurrent root release"
      end
    end
  end

  defmodule ConcurrentDependencyWorkflow.LoadInvoice do
    use Jido.Action,
      name: "load_invoice",
      description: "Blocks until the test releases the invoice root",
      schema: [
        invoice_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{invoice_id: invoice_id}, _context) do
      notify_test(:load_invoice)
      await_continue()
      {:ok, %{invoice: %{id: invoice_id, status: "open"}}}
    end

    defp notify_test(step_name) do
      case :persistent_term.get({ConcurrentDependencyWorkflow, :test_pid}, nil) do
        pid when is_pid(pid) -> send(pid, {:concurrent_root_started, step_name, self()})
        _other -> :ok
      end
    end

    defp await_continue do
      receive do
        :continue -> :ok
      after
        1_000 -> raise "timed out waiting for concurrent root release"
      end
    end
  end

  defmodule ConcurrentDependencyFailureWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, ConcurrentDependencyFailureWorkflow.LoadAccount
      step :load_invoice, ConcurrentDependencyFailureWorkflow.LoadInvoice
      step :send_email, DependencyWorkflow.SendEmail, after: [:load_account, :load_invoice]
    end
  end

  defmodule ConcurrentDependencyFailureWorkflow.LoadAccount do
    use Jido.Action,
      name: "load_account",
      description: "Completes after the test releases the successful root",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      notify_test(:load_account)
      await_continue()
      {:ok, %{account: %{id: account_id, tier: "pro"}}}
    end

    defp notify_test(step_name) do
      case :persistent_term.get({ConcurrentDependencyFailureWorkflow, :test_pid}, nil) do
        pid when is_pid(pid) -> send(pid, {:concurrent_root_started, step_name, self()})
        _other -> :ok
      end
    end

    defp await_continue do
      receive do
        :continue -> :ok
      after
        1_000 -> raise "timed out waiting for concurrent root release"
      end
    end
  end

  defmodule ConcurrentDependencyFailureWorkflow.LoadInvoice do
    use Jido.Action,
      name: "load_invoice",
      description: "Fails after the test releases the failing root",
      schema: [
        invoice_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{invoice_id: invoice_id}, _context) do
      notify_test(:load_invoice)
      await_continue()

      {:error,
       %{message: "invoice unavailable", code: "invoice_unavailable", invoice_id: invoice_id}}
    end

    defp notify_test(step_name) do
      case :persistent_term.get({ConcurrentDependencyFailureWorkflow, :test_pid}, nil) do
        pid when is_pid(pid) -> send(pid, {:concurrent_root_started, step_name, self()})
        _other -> :ok
      end
    end

    defp await_continue do
      receive do
        :continue -> :ok
      after
        1_000 -> raise "timed out waiting for concurrent root release"
      end
    end
  end

  defmodule ConcurrentRetryWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, ConcurrentRetryWorkflow.LoadAccount, retry: [max_attempts: 2]
      step :load_invoice, ConcurrentRetryWorkflow.LoadInvoice, retry: [max_attempts: 2]
      step :send_email, DependencyWorkflow.SendEmail, after: [:load_account, :load_invoice]
    end
  end

  defmodule ConcurrentRetryWorkflow.LoadAccount do
    use Jido.Action,
      name: "load_account",
      description: "Fails after the test releases the retryable account root",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      notify_test(:load_account)
      await_continue()

      {:error, %{message: "gateway timeout", code: "gateway_timeout", account_id: account_id}}
    end

    defp notify_test(step_name) do
      case :persistent_term.get({ConcurrentRetryWorkflow, :test_pid}, nil) do
        pid when is_pid(pid) -> send(pid, {:concurrent_root_started, step_name, self()})
        _other -> :ok
      end
    end

    defp await_continue do
      receive do
        :continue -> :ok
      after
        1_000 -> raise "timed out waiting for concurrent root release"
      end
    end
  end

  defmodule ConcurrentRetryWorkflow.LoadInvoice do
    use Jido.Action,
      name: "load_invoice",
      description: "Fails after the test releases the retryable invoice root",
      schema: [
        invoice_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{invoice_id: invoice_id}, _context) do
      notify_test(:load_invoice)
      await_continue()

      {:error, %{message: "gateway timeout", code: "gateway_timeout", invoice_id: invoice_id}}
    end

    defp notify_test(step_name) do
      case :persistent_term.get({ConcurrentRetryWorkflow, :test_pid}, nil) do
        pid when is_pid(pid) -> send(pid, {:concurrent_root_started, step_name, self()})
        _other -> :ok
      end
    end

    defp await_continue do
      receive do
        :continue -> :ok
      after
        1_000 -> raise "timed out waiting for concurrent root release"
      end
    end
  end

  defmodule DependencyFailureWorkflow.LoadInvoice do
    use Jido.Action,
      name: "load_invoice",
      description: "Fails while loading invoice details",
      schema: [
        invoice_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{invoice_id: invoice_id}, _context) do
      {:error,
       %{message: "invoice unavailable", code: "invoice_unavailable", invoice_id: invoice_id}}
    end
  end

  defmodule SuccessfulWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_invoice, SuccessfulWorkflow.LoadInvoice
      step :send_email, SuccessfulWorkflow.SendEmail

      transition :load_invoice, on: :ok, to: :send_email
      transition :send_email, on: :ok, to: :complete
    end
  end

  defmodule ExplicitMappingWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, ExplicitMappingWorkflow.LoadAccount,
        input: [:account_id],
        output: :account

      step :record_delivery, ExplicitMappingWorkflow.RecordDelivery,
        input: [:account, :invoice_id],
        output: :delivery

      transition :load_account, on: :ok, to: :record_delivery
      transition :record_delivery, on: :ok, to: :complete
    end
  end

  defmodule ExplicitMappingWorkflow.LoadAccount do
    use Jido.Action,
      name: "load_account",
      description: "Loads one account from an explicit input mapping",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{id: account_id}}
    end
  end

  defmodule ExplicitMappingWorkflow.RecordDelivery do
    use Jido.Action,
      name: "record_delivery",
      description: "Builds delivery output from explicitly mapped inputs",
      schema: [
        account: [type: :map, required: true],
        invoice_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account: account, invoice_id: invoice_id}, _context) do
      {:ok, %{account_id: account.id, invoice_id: invoice_id}}
    end
  end

  defmodule NamedPathMappingWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :reviewer_id, :string
        end
      end

      step :load_review_context, NamedPathMappingWorkflow.LoadReviewContext

      step :record_review, NamedPathMappingWorkflow.RecordReview,
        input: [
          drafts: [:draft, :drafts],
          reviewer: [:review_draft, :reviewer]
        ],
        output: :review

      transition :load_review_context, on: :ok, to: :record_review
      transition :record_review, on: :ok, to: :complete
    end
  end

  defmodule NamedPathMappingWorkflow.LoadReviewContext do
    use Jido.Action,
      name: "load_review_context",
      description: "Loads nested review context",
      schema: [
        account_id: [type: :string, required: true],
        reviewer_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{reviewer_id: reviewer_id}, _context) do
      {:ok,
       %{
         draft: %{drafts: [%{id: "draft_1"}, %{id: "draft_2"}]},
         review_draft: %{reviewer: %{id: reviewer_id}}
       }}
    end
  end

  defmodule NamedPathMappingWorkflow.RecordReview do
    use Jido.Action,
      name: "record_review",
      description: "Records review details from mapped nested input",
      schema: [
        drafts: [type: {:list, :map}, required: true],
        reviewer: [type: :map, required: true]
      ]

    @impl Jido.Action
    def run(%{drafts: drafts, reviewer: reviewer}, _context) do
      {:ok, %{draft_count: length(drafts), reviewer_id: reviewer.id}}
    end
  end

  defmodule NativeNamedPathMappingWorkflow.LoadReviewContext do
    use SquidMesh.Step,
      name: :load_review_context,
      input_schema: [
        account_id: [type: :string, required: true],
        reviewer_id: [type: :string, required: true]
      ],
      output_schema: [
        draft: [type: :map, required: true],
        review_draft: [type: :map, required: true]
      ]

    @impl SquidMesh.Step
    def run(%{reviewer_id: reviewer_id}, _context) do
      {:ok,
       %{
         draft: %{drafts: [%{id: "draft_1"}, %{id: "draft_2"}]},
         review_draft: %{reviewer: %{id: reviewer_id}}
       }}
    end
  end

  defmodule NativeNamedPathMappingWorkflow.RecordReview do
    use SquidMesh.Step,
      name: :record_review,
      input_schema: [
        drafts: [type: :list, required: true],
        reviewer: [type: :map, required: true]
      ],
      output_schema: [
        review: [type: :map, required: true]
      ]

    @impl SquidMesh.Step
    def run(%{drafts: drafts, reviewer: reviewer}, _context) do
      {:ok, %{review: %{draft_count: length(drafts), reviewer_id: reviewer.id}}}
    end
  end

  defmodule NativeNamedPathMappingWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :reviewer_id, :string
        end
      end

      step :load_review_context, NativeNamedPathMappingWorkflow.LoadReviewContext

      step :record_review, NativeNamedPathMappingWorkflow.RecordReview,
        input: [
          drafts: [:draft, :drafts],
          reviewer: [:review_draft, :reviewer]
        ]

      transition :load_review_context, on: :ok, to: :record_review
      transition :record_review, on: :ok, to: :complete
    end
  end

  defmodule MissingPathMappingWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :draft, :map
        end
      end

      step :record_review, MissingPathMappingWorkflow.RecordReview,
        input: [drafts: [:draft, :drafts]]

      transition :record_review, on: :ok, to: :complete
    end
  end

  defmodule MissingPathMappingWorkflow.RecordReview do
    use Jido.Action,
      name: "record_review",
      description: "Should not execute when mapped input is missing",
      schema: [drafts: [type: {:list, :map}, required: true]]

    @impl Jido.Action
    def run(_input, _context) do
      raise "record_review should not execute when mapped input is missing"
    end
  end

  defmodule SuccessMissingPathMappingWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :draft, :map
        end
      end

      step :load_review_context, SuccessMissingPathMappingWorkflow.LoadReviewContext

      step :record_review, SuccessMissingPathMappingWorkflow.RecordReview,
        after: [:load_review_context],
        input: [drafts: [:draft, :drafts]]
    end
  end

  defmodule SuccessMissingPathMappingWorkflow.LoadReviewContext do
    use Jido.Action,
      name: "load_review_context",
      description: "Returns a partial nested context",
      schema: [draft: [type: :map, required: true]]

    @impl Jido.Action
    def run(%{draft: draft}, _context), do: {:ok, %{draft: draft}}
  end

  defmodule SuccessMissingPathMappingWorkflow.RecordReview do
    use Jido.Action,
      name: "record_review",
      description: "Should not execute when successor mapped input is missing",
      schema: [drafts: [type: {:list, :map}, required: true]]

    @impl Jido.Action
    def run(_input, _context) do
      raise "record_review should not execute when successor mapped input is missing"
    end
  end

  defmodule SuccessfulWorkflow.LoadInvoice do
    use Jido.Action,
      name: "load_invoice",
      description: "Loads invoice details",
      schema: [
        account_id: [type: :string, required: true],
        invoice_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id, invoice_id: invoice_id}, _context) do
      {:ok,
       %{
         account: %{id: account_id},
         invoice: %{id: invoice_id, status: "open"}
       }}
    end
  end

  defmodule SuccessfulWorkflow.SendEmail do
    use Jido.Action,
      name: "send_email",
      description: "Sends a recovery email",
      schema: [
        account: [type: :map, required: true],
        invoice: [type: :map, required: true]
      ]

    @impl Jido.Action
    def run(%{account: account, invoice: invoice}, _context) do
      {:ok,
       %{
         delivery: %{
           account_id: account.id,
           invoice_id: invoice.id,
           channel: "email"
         }
       }}
    end
  end

  defmodule FailingWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :check_gateway, FailingWorkflow.CheckGateway
      transition :check_gateway, on: :ok, to: :complete
    end
  end

  defmodule FailingWorkflow.CheckGateway do
    use Jido.Action,
      name: "check_gateway",
      description: "Checks gateway availability",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(_params, _context) do
      {:error, %{message: "gateway timeout", code: "gateway_timeout"}}
    end
  end

  defmodule TransactionalSuccessWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :write_local_records, TransactionalSuccessWorkflow.WriteLocalRecords,
        transaction: :repo

      transition :write_local_records, on: :ok, to: :complete
    end
  end

  defmodule TransactionalFailureWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :write_local_records, TransactionalFailureWorkflow.WriteLocalRecords,
        transaction: :repo

      transition :write_local_records, on: :ok, to: :complete
    end
  end

  defmodule TransactionalSuccessWorkflow.WriteLocalRecords do
    use Jido.Action,
      name: "write_local_records",
      description: "Writes a local group of records",
      schema: [
        account_id: [type: :string, required: true]
      ]

    alias SquidMesh.Test.Repo

    @impl Jido.Action
    def run(%{account_id: account_id}, %{run_id: run_id}) do
      write_events!(run_id, account_id, ["reserved", "captured"])
      {:ok, %{local_transaction: %{status: "committed", rows: 2}}}
    end

    defp write_events!(run_id, account_id, events) do
      dumped_run_id = Ecto.UUID.dump!(run_id)
      now = NaiveDateTime.utc_now(:second)

      entries =
        Enum.map(events, fn event ->
          %{
            run_id: dumped_run_id,
            account_id: account_id,
            event: event,
            inserted_at: now,
            updated_at: now
          }
        end)

      {2, nil} = Repo.insert_all("transactional_events", entries)
      :ok
    end
  end

  defmodule TransactionalFailureWorkflow.WriteLocalRecords do
    use Jido.Action,
      name: "write_local_records",
      description: "Writes a local group of records before failing",
      schema: [
        account_id: [type: :string, required: true]
      ]

    alias SquidMesh.Test.Repo

    @impl Jido.Action
    def run(%{account_id: account_id}, %{run_id: run_id}) do
      write_events!(run_id, account_id, ["reserved", "captured"])
      {:error, %{message: "local group failed"}}
    end

    defp write_events!(run_id, account_id, events) do
      dumped_run_id = Ecto.UUID.dump!(run_id)
      now = NaiveDateTime.utc_now(:second)

      entries =
        Enum.map(events, fn event ->
          %{
            run_id: dumped_run_id,
            account_id: account_id,
            event: event,
            inserted_at: now,
            updated_at: now
          }
        end)

      {2, nil} = Repo.insert_all("transactional_events", entries)
      :ok
    end
  end

  defmodule CompensationWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :order_id, :string
        end
      end

      step :hold_credit, CompensationWorkflow.HoldCredit,
        compensate: CompensationWorkflow.ReleaseCreditHold

      step :reserve_inventory, CompensationWorkflow.ReserveInventory,
        compensate: CompensationWorkflow.ReleaseInventory

      step :charge_card, CompensationWorkflow.ChargeCard

      transition :hold_credit, on: :ok, to: :reserve_inventory
      transition :reserve_inventory, on: :ok, to: :charge_card
      transition :charge_card, on: :ok, to: :complete
    end
  end

  defmodule CompensationWorkflow.HoldCredit do
    use Jido.Action,
      name: "hold_credit",
      description: "Places a credit hold",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{credit_hold: %{account_id: account_id, status: "held"}}}
    end
  end

  defmodule CompensationWorkflow.ReserveInventory do
    use Jido.Action,
      name: "reserve_inventory",
      description: "Reserves order inventory",
      schema: [
        order_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{order_id: order_id}, _context) do
      {:ok, %{inventory_reservation: %{order_id: order_id, status: "reserved"}}}
    end
  end

  defmodule CompensationWorkflow.ChargeCard do
    use Jido.Action,
      name: "charge_card",
      description: "Attempts to charge a card",
      schema: []

    @impl Jido.Action
    def run(_params, _context) do
      {:error, %{message: "card declined", code: "card_declined"}}
    end
  end

  defmodule CompensationWorkflow.ReleaseCreditHold do
    use SquidMesh.Step,
      name: :release_credit_hold,
      description: "Releases a prior credit hold",
      input_schema: [
        payload: [type: :map, required: true]
      ],
      output_schema: [
        released: [type: :string, required: true]
      ]

    @impl SquidMesh.Step
    def run(%{payload: %{account_id: account_id}}, _context) do
      if :persistent_term.get({CompensationWorkflow, :fail_release_credit?}, false) do
        {:error, %{message: "release failed", code: "release_failed"}}
      else
        events = :persistent_term.get({CompensationWorkflow, :events}, [])

        :persistent_term.put(
          {CompensationWorkflow, :events},
          List.insert_at(events, -1, {:release_credit_hold, account_id})
        )

        {:ok, %{released: "credit"}}
      end
    end
  end

  defmodule CompensationWorkflow.ReleaseInventory do
    use SquidMesh.Step,
      name: :release_inventory,
      description: "Releases a prior inventory reservation",
      input_schema: [
        payload: [type: :map, required: true]
      ],
      output_schema: [
        released: [type: :string, required: true]
      ]

    @impl SquidMesh.Step
    def run(%{payload: %{order_id: order_id}}, _context) do
      events = :persistent_term.get({CompensationWorkflow, :events}, [])

      :persistent_term.put(
        {CompensationWorkflow, :events},
        List.insert_at(events, -1, {:release_inventory, order_id})
      )

      {:ok, %{released: "inventory"}}
    end
  end

  defmodule CompensationRetryWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :hold_credit, CompensationRetryWorkflow.HoldCredit,
        compensate: CompensationRetryWorkflow.ReleaseCreditHold

      step :charge_card, CompensationRetryWorkflow.ChargeCard, retry: [max_attempts: 2]

      transition :hold_credit, on: :ok, to: :charge_card
      transition :charge_card, on: :ok, to: :complete
    end
  end

  defmodule CompensationRetryWorkflow.HoldCredit do
    use Jido.Action,
      name: "hold_credit",
      description: "Places a credit hold",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{credit_hold: %{account_id: account_id, status: "held"}}}
    end
  end

  defmodule CompensationRetryWorkflow.ChargeCard do
    use Jido.Action,
      name: "charge_card",
      description: "Attempts to charge a card with workflow retries",
      schema: []

    @impl Jido.Action
    def run(_params, _context) do
      {:error, %{message: "card declined", code: "card_declined"}}
    end
  end

  defmodule CompensationRetryWorkflow.ReleaseCreditHold do
    use Jido.Action,
      name: "release_credit_hold_retry",
      description: "Releases a prior credit hold",
      schema: []

    @impl Jido.Action
    def run(%{payload: %{account_id: account_id}}, _context) do
      events = :persistent_term.get({CompensationRetryWorkflow, :events}, [])

      :persistent_term.put(
        {CompensationRetryWorkflow, :events},
        List.insert_at(events, -1, {:release_credit_hold, account_id})
      )

      {:ok, %{released: "credit"}}
    end
  end

  defmodule BackoffWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :check_gateway, BackoffWorkflow.CheckGateway,
        retry: [max_attempts: 3, backoff: [type: :exponential, min: 1_000, max: 5_000]]

      transition :check_gateway, on: :ok, to: :complete
    end
  end

  defmodule BackoffWorkflow.CheckGateway do
    use Jido.Action,
      name: "check_gateway",
      description: "Checks gateway availability with retry backoff",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(_params, _context) do
      {:error, %{message: "gateway timeout", code: "gateway_timeout"}}
    end
  end

  defmodule ErrorRoutingWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :check_gateway, ErrorRoutingWorkflow.CheckGateway
      step :queue_recovery, ErrorRoutingWorkflow.QueueRecovery

      transition :check_gateway, on: :error, to: :queue_recovery, recovery: :compensation
      transition :queue_recovery, on: :ok, to: :complete
    end
  end

  defmodule ErrorRoutingWorkflow.CheckGateway do
    use Jido.Action,
      name: "check_gateway",
      description: "Fails and routes to recovery",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(_params, _context) do
      {:error, %{message: "gateway timeout", code: "gateway_timeout"}}
    end
  end

  defmodule ErrorRoutingWorkflow.QueueRecovery do
    use Jido.Action,
      name: "queue_recovery",
      description: "Queues a recovery action after failure routing",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{recovery: %{account_id: account_id, status: "queued"}}}
    end
  end

  defmodule UndoRoutingWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :reserve_inventory, UndoRoutingWorkflow.ReserveInventory
      step :release_inventory, UndoRoutingWorkflow.ReleaseInventory

      transition :reserve_inventory, on: :error, to: :release_inventory, recovery: :undo
      transition :release_inventory, on: :ok, to: :complete
    end
  end

  defmodule UndoRoutingWorkflow.ReserveInventory do
    use Jido.Action,
      name: "reserve_inventory",
      description: "Fails after a reservation attempt",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(_params, _context) do
      {:error, %{message: "reservation failed", code: "reservation_failed"}}
    end
  end

  defmodule UndoRoutingWorkflow.ReleaseInventory do
    use Jido.Action,
      name: "release_inventory",
      description: "Releases a local reservation after a failed attempt",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{undo: %{account_id: account_id, status: "released"}}}
    end
  end

  defmodule CompleteUndoRoutingWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :reserve_inventory, CompleteUndoRoutingWorkflow.ReserveInventory

      transition :reserve_inventory, on: :error, to: :complete, recovery: :undo
    end
  end

  defmodule CompleteUndoRoutingWorkflow.ReserveInventory do
    use Jido.Action,
      name: "reserve_inventory",
      description: "Fails after a reservation attempt that needs no follow-up step",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(_params, _context) do
      {:error, %{message: "reservation failed", code: "reservation_failed"}}
    end
  end

  defmodule ExhaustedRetryWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :check_gateway, ExhaustedRetryWorkflow.CheckGateway, retry: [max_attempts: 2]
      step :queue_recovery, ExhaustedRetryWorkflow.QueueRecovery

      transition :check_gateway, on: :error, to: :queue_recovery
      transition :queue_recovery, on: :ok, to: :complete
    end
  end

  defmodule ExhaustedRetryWorkflow.CheckGateway do
    use Jido.Action,
      name: "check_gateway",
      description: "Fails until retries are exhausted and error routing can continue",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(_params, _context) do
      {:error, %{message: "gateway timeout", code: "gateway_timeout"}}
    end
  end

  defmodule ExhaustedRetryWorkflow.QueueRecovery do
    use Jido.Action,
      name: "queue_recovery",
      description: "Queues recovery after retries are exhausted",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{recovery: %{account_id: account_id, status: "queued"}}}
    end
  end

  defmodule RetrySurfaceWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :check_gateway, RetrySurfaceWorkflow.FailOnce,
        retry: [max_attempts: 3, backoff: [type: :exponential, min: 1_000, max: 5_000]]

      transition :check_gateway, on: :ok, to: :complete
    end
  end

  defmodule RetrySurfaceWorkflow.FailOnce do
    use Jido.Action,
      name: "check_gateway",
      description: "Fails once so Squid Mesh owns the retry boundary",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @coordination_key {__MODULE__, :attempts}

    @impl Jido.Action
    def run(%{account_id: account_id}, %{run_id: run_id}) do
      seen_runs = :persistent_term.get(@coordination_key, MapSet.new())

      if MapSet.member?(seen_runs, run_id) do
        {:ok, %{gateway_check: %{account_id: account_id, status: "ok"}}}
      else
        :persistent_term.put(@coordination_key, MapSet.put(seen_runs, run_id))
        {:error, %{message: "gateway timeout", code: "gateway_timeout"}}
      end
    end
  end

  defmodule BuiltInWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :wait_for_settlement, :wait, duration: 10
      step :log_delivery, :log, message: "delivery completed", level: :info

      transition :wait_for_settlement, on: :ok, to: :log_delivery
      transition :log_delivery, on: :ok, to: :complete
    end
  end

  defmodule PauseWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :wait_for_approval, :pause
      step :record_delivery, :log, message: "delivery recorded", level: :info

      transition :wait_for_approval, on: :ok, to: :record_delivery
      transition :record_delivery, on: :ok, to: :complete
    end
  end

  defmodule PauseMappedWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :wait_for_approval, :pause, output: :approval

      step :record_delivery, PauseMappedWorkflow.RecordDelivery,
        input: [:approval, :account_id],
        output: :delivery

      transition :wait_for_approval, on: :ok, to: :record_delivery
      transition :record_delivery, on: :ok, to: :complete
    end
  end

  defmodule PauseMappedWorkflow.RecordDelivery do
    use Jido.Action,
      name: "record_delivery",
      description: "Confirms pause output mappings flow into the resumed step",
      schema: [
        approval: [type: :map, required: true],
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{approval: approval, account_id: account_id}, _context) do
      {:ok, %{account_id: account_id, approval: approval}}
    end
  end

  defmodule ApprovalWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      approval_step :wait_for_review, output: :approval

      step :record_approval, ApprovalWorkflow.RecordApproval,
        input: [:approval, :account_id],
        output: :approved

      step :record_rejection, ApprovalWorkflow.RecordRejection,
        input: [:approval, :account_id],
        output: :rejected

      transition :wait_for_review, on: :ok, to: :record_approval
      transition :wait_for_review, on: :error, to: :record_rejection
      transition :record_approval, on: :ok, to: :complete
      transition :record_rejection, on: :ok, to: :complete
    end
  end

  defmodule ApprovalWorkflow.RecordApproval do
    use Jido.Action,
      name: "record_approval",
      description: "Persists approval decisions after review",
      schema: [
        approval: [type: :map, required: true],
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{approval: approval, account_id: account_id}, _context) do
      {:ok,
       %{
         account_id: account_id,
         actor: approval.actor,
         decision: approval.decision
       }}
    end
  end

  defmodule ApprovalWorkflow.RecordRejection do
    use Jido.Action,
      name: "record_rejection",
      description: "Persists rejection decisions after review",
      schema: [
        approval: [type: :map, required: true],
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{approval: approval, account_id: account_id}, _context) do
      {:ok,
       %{
         account_id: account_id,
         actor: approval.actor,
         decision: approval.decision
       }}
    end
  end

  defmodule CancellationCompletionWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :wait_for_settlement, :wait, duration: 10
      step :record_delivery, CancellationCompletionWorkflow.RecordDelivery

      transition :wait_for_settlement, on: :ok, to: :record_delivery
      transition :record_delivery, on: :ok, to: :complete
    end
  end

  defmodule CancellationCompletionWorkflow.RecordDelivery do
    use Jido.Action,
      name: "record_delivery",
      description: "Blocks until the test allows completion",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @coordination_key {__MODULE__, :test_pid}

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      test_pid = :persistent_term.get(@coordination_key)
      send(test_pid, {:record_delivery_started, self(), account_id})

      receive do
        :continue -> {:ok, %{delivery: %{account_id: account_id, status: "recorded"}}}
      after
        5_000 -> {:error, %{message: "timed out waiting for test continuation"}}
      end
    end
  end

  defmodule NativeStepWorkflow.LoadAccount do
    use SquidMesh.Step,
      name: :load_account,
      input_schema: [
        account_id: [type: :string, required: true]
      ],
      output_schema: [
        id: [type: :string, required: true],
        source_step: [type: :atom, required: true]
      ]

    @impl SquidMesh.Step
    def run(%{account_id: account_id}, %SquidMesh.Step.Context{step: step}) do
      {:ok, %{id: account_id, source_step: step}}
    end
  end

  defmodule NativeStepWorkflow.RecordDelivery do
    use SquidMesh.Step,
      name: :record_delivery,
      input_schema: [
        account: [type: :map, required: true]
      ],
      output_schema: [
        account_id: [type: :string, required: true],
        invoice_id: [type: :string, required: true],
        attempt: [type: :integer, required: true]
      ]

    @impl SquidMesh.Step
    def run(%{account: account}, %SquidMesh.Step.Context{state: state, attempt: attempt}) do
      {:ok,
       %{
         account_id: account.id,
         invoice_id: state.invoice_id,
         attempt: attempt
       }}
    end
  end

  defmodule NativeStepWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, NativeStepWorkflow.LoadAccount,
        input: [:account_id],
        output: :account

      step :record_delivery, NativeStepWorkflow.RecordDelivery,
        input: [:account],
        output: :delivery

      transition :load_account, on: :ok, to: :record_delivery
      transition :record_delivery, on: :ok, to: :complete
    end
  end

  defmodule NativeStepErrorRoutingWorkflow.ChargeCard do
    use SquidMesh.Step,
      name: :charge_card,
      input_schema: [
        account_id: [type: :string, required: true]
      ]

    @impl SquidMesh.Step
    def run(_input, _context) do
      {:error, %{message: "card_declined", code: "card_declined"}}
    end
  end

  defmodule NativeStepErrorRoutingWorkflow.QueueRecovery do
    use SquidMesh.Step,
      name: :queue_recovery,
      input_schema: [
        account_id: [type: :string, required: true]
      ]

    @impl SquidMesh.Step
    def run(%{account_id: account_id}, _context) do
      {:ok, %{recovery: %{account_id: account_id, reason: :declined}}}
    end
  end

  defmodule NativeStepErrorRoutingWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :charge_card, NativeStepErrorRoutingWorkflow.ChargeCard, retry: [max_attempts: 3]

      step :queue_recovery, NativeStepErrorRoutingWorkflow.QueueRecovery

      transition :charge_card, on: :error, to: :queue_recovery
      transition :queue_recovery, on: :ok, to: :complete
    end
  end

  defmodule NativeStepRetryWorkflow.CheckGateway do
    use SquidMesh.Step,
      name: :check_gateway,
      input_schema: [
        account_id: [type: :string, required: true]
      ]

    @coordination_key {__MODULE__, :attempts}

    @impl SquidMesh.Step
    def run(%{account_id: account_id}, %SquidMesh.Step.Context{run_id: run_id}) do
      seen_runs = :persistent_term.get(@coordination_key, MapSet.new())

      if MapSet.member?(seen_runs, run_id) do
        {:ok, %{gateway_check: %{account_id: account_id, status: :ok}}}
      else
        :persistent_term.put(@coordination_key, MapSet.put(seen_runs, run_id))

        {:retry,
         %{
           message: "gateway_timeout",
           code: "gateway_timeout"
         }}
      end
    end
  end

  defmodule NativeStepRetryWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :check_gateway, NativeStepRetryWorkflow.CheckGateway, retry: [max_attempts: 2]

      transition :check_gateway, on: :ok, to: :complete
    end
  end

  defmodule NativeStepScheduledSuccessWorkflow.ScheduleNext do
    use SquidMesh.Step,
      name: :schedule_next,
      input_schema: [
        account_id: [type: :string, required: true]
      ],
      output_schema: [
        scheduled: [type: :map, required: true]
      ]

    @impl SquidMesh.Step
    def run(%{account_id: account_id}, _context) do
      {:ok, %{scheduled: %{account_id: account_id}}, schedule_in: 60}
    end
  end

  defmodule NativeStepScheduledSuccessWorkflow.RecordDelivery do
    use SquidMesh.Step,
      name: :record_delivery,
      input_schema: [
        scheduled: [type: :map, required: true]
      ]

    @impl SquidMesh.Step
    def run(%{scheduled: scheduled}, _context), do: {:ok, %{delivered: scheduled}}
  end

  defmodule NativeStepScheduledSuccessWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :schedule_next, NativeStepScheduledSuccessWorkflow.ScheduleNext
      step :record_delivery, NativeStepScheduledSuccessWorkflow.RecordDelivery

      transition :schedule_next, on: :ok, to: :record_delivery
      transition :record_delivery, on: :ok, to: :complete
    end
  end

  defmodule NativeStepRetryAfterWorkflow.CheckGateway do
    use SquidMesh.Step,
      name: :check_gateway,
      input_schema: [
        account_id: [type: :string, required: true]
      ]

    @impl SquidMesh.Step
    def run(_input, _context) do
      {:retry, %{message: "gateway_timeout", code: "gateway_timeout"}, retry_after: 60_000}
    end
  end

  defmodule NativeStepRetryAfterWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :check_gateway, NativeStepRetryAfterWorkflow.CheckGateway, retry: [max_attempts: 2]

      transition :check_gateway, on: :ok, to: :complete
    end
  end

  defmodule NativeStepValidationRecovery do
    use SquidMesh.Step,
      name: :queue_recovery,
      input_schema: [
        account_id: [type: :string, required: true]
      ]

    @impl SquidMesh.Step
    def run(%{account_id: account_id}, _context) do
      {:ok, %{recovery: %{account_id: account_id, reason: :validation}}}
    end
  end

  defmodule NativeStepInputValidationWorkflow.LoadInvoice do
    use SquidMesh.Step,
      name: :load_invoice,
      input_schema: [
        invoice_id: [type: :string, required: true]
      ]

    @impl SquidMesh.Step
    def run(_input, _context), do: {:ok, %{}}
  end

  defmodule NativeStepInputValidationWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :load_invoice, NativeStepInputValidationWorkflow.LoadInvoice, retry: [max_attempts: 2]
      step :queue_recovery, NativeStepValidationRecovery

      transition :load_invoice, on: :error, to: :queue_recovery
      transition :queue_recovery, on: :ok, to: :complete
    end
  end

  defmodule NativeStepOutputValidationWorkflow.LoadAccount do
    use SquidMesh.Step,
      name: :load_account,
      input_schema: [
        account_id: [type: :string, required: true]
      ],
      output_schema: [
        account: [type: :map, required: true]
      ]

    @impl SquidMesh.Step
    def run(%{account_id: account_id}, _context), do: {:ok, %{account_id: account_id}}
  end

  defmodule NativeStepOutputValidationWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :load_account, NativeStepOutputValidationWorkflow.LoadAccount, retry: [max_attempts: 2]
      step :queue_recovery, NativeStepValidationRecovery

      transition :load_account, on: :error, to: :queue_recovery
      transition :queue_recovery, on: :ok, to: :complete
    end
  end

  defmodule MissingExecutor do
    @behaviour SquidMesh.Executor

    @impl SquidMesh.Executor
    def enqueue_step(_config, _run, _step, _opts), do: {:error, :executor_unavailable}

    @impl SquidMesh.Executor
    def enqueue_steps(_config, _run, _steps, _opts), do: {:error, :executor_unavailable}

    @impl SquidMesh.Executor
    def enqueue_compensation(_config, _run, _opts), do: {:error, :executor_unavailable}

    @impl SquidMesh.Executor
    def enqueue_cron(_config, _workflow, _trigger, _opts), do: {:error, :executor_unavailable}
  end
end
