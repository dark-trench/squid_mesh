defmodule SquidMesh.Runs.ExplanationTest do
  use SquidMesh.DataCase, async: false

  alias __MODULE__.ApprovalWorkflow
  alias __MODULE__.BackoffWorkflow
  alias __MODULE__.DependencyWorkflow
  alias __MODULE__.IrreversibleWorkflow
  alias __MODULE__.PauseWorkflow
  alias __MODULE__.RetryExhaustedWorkflow
  alias __MODULE__.SuccessfulWorkflow
  alias SquidMesh.Runs.Explanation
  alias SquidMesh.Steps
  alias SquidMesh.Test.Executor
  alias SquidMesh.Test.Job
  alias SquidMesh.Test.StepWorker

  describe "explain_run/2" do
    test "returns not found when the run does not exist" do
      assert {:error, :not_found} =
               SquidMesh.explain_run(Ecto.UUID.generate(), repo: Repo)
    end

    test "returns a structured error for malformed run ids" do
      assert {:error, :invalid_run_id} =
               SquidMesh.explain_run("not-a-uuid", repo: Repo)
    end

    test "returns invalid configuration errors from the public API" do
      assert {:error, {:invalid_config, [stale_step_timeout: -1]}} =
               SquidMesh.explain_run(Ecto.UUID.generate(),
                 repo: Repo,
                 stale_step_timeout: -1
               )
    end

    test "runtime-table read model ignores projection-only options" do
      assert {:ok, run} =
               SquidMesh.start_run(SuccessfulWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert {:ok, %Explanation{} = explanation} =
               SquidMesh.explain_run(run.id,
                 read_model: :runtime_tables,
                 journal_storage: :not_used_by_runtime_tables,
                 queue: "default",
                 now: ~U[2026-05-15 00:00:00Z],
                 repo: Repo
               )

      assert explanation.status == :pending
    end

    test "explains a failed run whose step exhausted retries" do
      assert {:ok, run} =
               SquidMesh.start_run(RetryExhaustedWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: success, failure: 0} =
               Executor.drain()

      assert success >= 2

      assert {:ok, %Explanation{} = explanation} =
               SquidMesh.explain_run(run.id, repo: Repo)

      assert explanation.status == :failed
      assert explanation.reason == :retry_exhausted
      assert explanation.step == :check_gateway
      assert explanation.details.max_attempts == 2
      assert explanation.details.latest_attempt_number == 2
      assert explanation.next_actions == [:replay_run]

      assert %{run: %{status: :failed}, step_run: %{status: :failed}, attempt: %{status: :failed}} =
               explanation.evidence
    end

    test "explains manual pause and approval waits from persisted resume metadata" do
      assert {:ok, pause_run} =
               SquidMesh.start_run(PauseWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => pause_run.id, "step" => "wait_for_approval"}
               })

      assert {:ok, pause_explanation} = SquidMesh.explain_run(pause_run.id, repo: Repo)

      assert pause_explanation.status == :paused
      assert pause_explanation.reason == :paused_for_manual_action
      assert pause_explanation.step == :wait_for_approval
      assert pause_explanation.details.resume_target == :record_delivery
      assert pause_explanation.next_actions == [:unblock_run, :cancel_run]
      assert pause_explanation.evidence.step_run.resume.kind == :pause

      assert {:ok, approval_run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_456"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => approval_run.id, "step" => "wait_for_review"}
               })

      assert {:ok, approval_explanation} = SquidMesh.explain_run(approval_run.id, repo: Repo)

      assert approval_explanation.status == :paused
      assert approval_explanation.reason == :paused_for_approval
      assert approval_explanation.step == :wait_for_review

      assert approval_explanation.details.approval_targets == %{
               ok: :record_approval,
               error: :record_rejection
             }

      assert approval_explanation.next_actions == [:approve_run, :reject_run, :cancel_run]
      assert approval_explanation.evidence.step_run.resume.kind == :approval
    end

    test "explains invalid persisted resume metadata without recomputing the workflow definition" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      assert {1, _rows} =
               Repo.update_all(
                 from(step_run in SquidMesh.Persistence.StepRun,
                   where:
                     step_run.run_id == ^run.id and step_run.step == "wait_for_approval" and
                       step_run.status == "running"
                 ),
                 set: [resume: %{"output" => %{}, "target" => "missing_step"}]
               )

      assert {:ok, explanation} = SquidMesh.explain_run(run.id, repo: Repo)

      assert explanation.status == :paused
      assert explanation.reason == :paused_with_invalid_resume_target
      assert explanation.step == :wait_for_approval
      assert explanation.details.resume_target == "missing_step"
      assert explanation.next_actions == [:cancel_run]
    end

    test "does not advertise manual actions when a paused run workflow cannot load" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {1, _rows} =
               Repo.update_all(
                 from(run_record in SquidMesh.Persistence.Run, where: run_record.id == ^run.id),
                 set: [workflow: "Elixir.Missing.Workflow"]
               )

      assert {:ok, explanation} = SquidMesh.explain_run(run.id, repo: Repo)

      assert explanation.status == :paused
      assert explanation.reason == :paused_with_unavailable_workflow
      assert explanation.step == "wait_for_review"
      assert explanation.details.workflow_definition == :unavailable
      assert explanation.next_actions == [:cancel_run]
      assert explanation.evidence.workflow_definition == %{available?: false}
      refute :approve_run in explanation.next_actions
      refute :reject_run in explanation.next_actions
    end

    test "explains retrying runs waiting for a scheduled retry job" do
      assert {:ok, run} =
               SquidMesh.start_run(BackoffWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: 1, failure: 0} = Executor.drain()

      assert {:ok, explanation} = SquidMesh.explain_run(run.id, repo: Repo)

      assert explanation.status == :retrying
      assert explanation.reason == :waiting_for_retry
      assert explanation.step == :check_gateway
      assert explanation.details.next_attempt_number == 2
      assert explanation.next_actions == [:wait_for_retry, :cancel_run]
      assert explanation.evidence.attempt.attempt_number == 1
    end

    test "explains dependency runs waiting on prerequisite steps" do
      assert {:ok, run} =
               SquidMesh.start_run(DependencyWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "load_account"}
               })

      assert {:ok, explanation} = SquidMesh.explain_run(run.id, repo: Repo)

      assert explanation.status == :running
      assert explanation.reason == :waiting_for_dependencies
      assert explanation.step == :send_email
      assert explanation.details.waiting_on == [:load_invoice]

      assert explanation.details.dependency_statuses == %{
               load_account: :completed,
               load_invoice: :pending
             }

      assert explanation.next_actions == [:wait_for_dependencies, :cancel_run]

      assert %{steps: [%{step: :load_account}, %{step: :load_invoice}, %{step: :send_email}]} =
               explanation.evidence
    end

    test "explains dependency joins scheduled after prerequisites complete" do
      assert {:ok, run} =
               SquidMesh.start_run(DependencyWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "load_account"}
               })

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "load_invoice"}
               })

      assert {:ok, explanation} = SquidMesh.explain_run(run.id, repo: Repo)

      assert explanation.status == :running
      assert explanation.reason == :step_scheduled
      assert explanation.step == :send_email
      assert explanation.details.satisfied_dependencies == [:load_account, :load_invoice]
      assert explanation.next_actions == [:wait_for_step, :cancel_run]

      assert %{
               steps: [
                 %{step: :load_account, status: :completed},
                 %{step: :load_invoice, status: :completed},
                 %{step: :send_email, status: :pending}
               ]
             } = explanation.evidence
    end

    test "explains running, cancelling, cancelled, and completed states" do
      assert {:ok, running_run} =
               SquidMesh.start_run(SuccessfulWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => running_run.id, "step" => "load_account"}
               })

      assert {:ok, running_explanation} = SquidMesh.explain_run(running_run.id, repo: Repo)

      assert running_explanation.status == :running
      assert running_explanation.reason == :step_scheduled
      assert running_explanation.step == :send_email
      assert running_explanation.next_actions == [:wait_for_step, :cancel_run]

      assert {:ok, cancellable_run} =
               SquidMesh.start_run(SuccessfulWorkflow, %{account_id: "acct_456"}, repo: Repo)

      assert {:ok, _step_run, :execute} =
               Steps.Store.begin_step(Repo, cancellable_run.id, :load_account, %{
                 account_id: "acct_456"
               })

      assert {:ok, _running} =
               SquidMesh.Runs.Store.transition_run(Repo, cancellable_run.id, :running, %{
                 current_step: :load_account
               })

      assert {:ok, cancelling_run} = SquidMesh.cancel_run(cancellable_run.id, repo: Repo)
      assert cancelling_run.status == :cancelling

      assert {:ok, cancelling_explanation} = SquidMesh.explain_run(cancellable_run.id, repo: Repo)

      assert cancelling_explanation.status == :cancelling
      assert cancelling_explanation.reason == :cancelling
      assert cancelling_explanation.next_actions == [:wait_for_cancellation]

      assert {:ok, cancel_run} =
               SquidMesh.start_run(SuccessfulWorkflow, %{account_id: "acct_789"}, repo: Repo)

      assert {:ok, cancelled_run} = SquidMesh.cancel_run(cancel_run.id, repo: Repo)
      assert cancelled_run.status == :cancelled

      assert {:ok, cancelled_explanation} = SquidMesh.explain_run(cancel_run.id, repo: Repo)

      assert cancelled_explanation.status == :cancelled
      assert cancelled_explanation.reason == :cancelled
      assert cancelled_explanation.next_actions == [:replay_run]

      assert {:ok, completed_run} =
               SquidMesh.start_run(SuccessfulWorkflow, %{account_id: "acct_999"}, repo: Repo)

      assert %{success: success, failure: 0} =
               Executor.drain()

      assert success >= 2

      assert {:ok, completed_explanation} = SquidMesh.explain_run(completed_run.id, repo: Repo)

      assert completed_explanation.status == :completed
      assert completed_explanation.reason == :completed
      assert completed_explanation.next_actions == [:replay_run]
    end

    test "omits replay actions for terminal runs whose workflow cannot load" do
      assert {:ok, completed_run} =
               SquidMesh.start_run(SuccessfulWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: success, failure: 0} =
               Executor.drain()

      assert success >= 2

      assert {1, _rows} =
               Repo.update_all(
                 from(run_record in SquidMesh.Persistence.Run,
                   where: run_record.id == ^completed_run.id
                 ),
                 set: [workflow: "Elixir.Missing.Workflow"]
               )

      assert {:ok, explanation} = SquidMesh.explain_run(completed_run.id, repo: Repo)

      assert explanation.status == :completed
      assert explanation.reason == :completed
      assert explanation.details.workflow_definition == :unavailable
      assert explanation.next_actions == []
      assert explanation.evidence.workflow_definition == %{available?: false}
      refute :replay_run in explanation.next_actions
    end

    test "requires explicit replay approval after completed irreversible steps" do
      assert {:ok, completed_run} =
               SquidMesh.start_run(IrreversibleWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert %{success: 2, failure: 0} =
               Executor.drain()

      assert {:ok, explanation} = SquidMesh.explain_run(completed_run.id, repo: Repo)

      assert explanation.status == :completed
      assert explanation.reason == :completed
      assert explanation.next_actions == []

      assert explanation.details.replay == %{
               allowed?: false,
               required_override: :allow_irreversible,
               blocked_by: [
                 %{
                   step: :capture_payment,
                   irreversible?: true,
                   compensatable?: false,
                   replay: :manual_review_required,
                   recovery: :manual_intervention
                 }
               ]
             }
    end

    test "surfaces stale running step recovery policy for duplicate delivery skips" do
      assert {:ok, run} =
               SquidMesh.start_run(SuccessfulWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert {:ok, _step_run, :execute} =
               Steps.Store.begin_step(Repo, run.id, :load_account, %{account_id: "acct_123"})

      assert {:ok, _running} =
               SquidMesh.Runs.Store.transition_run(Repo, run.id, :running, %{
                 current_step: :load_account
               })

      assert {:ok, explanation} =
               SquidMesh.explain_run(run.id,
                 repo: Repo,
                 stale_step_timeout: 30_000
               )

      assert explanation.status == :running
      assert explanation.reason == :step_running
      assert explanation.step == :load_account
      assert explanation.details.duplicate_delivery_policy == :skip_while_running
      assert explanation.details.stale_step_reclaim == %{enabled: true, timeout_ms: 30_000}
    end
  end

  defmodule SuccessfulWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :load_account, SuccessfulWorkflow.LoadAccount
      step :send_email, SuccessfulWorkflow.SendEmail

      transition :load_account, on: :ok, to: :send_email
      transition :send_email, on: :ok, to: :complete
    end
  end

  defmodule SuccessfulWorkflow.LoadAccount do
    use Jido.Action,
      name: "load_account",
      description: "Loads account details",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{account: %{id: account_id}}}
    end
  end

  defmodule SuccessfulWorkflow.SendEmail do
    use Jido.Action,
      name: "send_email",
      description: "Sends a message",
      schema: [account: [type: :map, required: true]]

    @impl Jido.Action
    def run(%{account: account}, _context) do
      {:ok, %{delivery: %{account_id: account.id}}}
    end
  end

  defmodule IrreversibleWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field(:account_id, :string)
        end
      end

      step(:load_account, IrreversibleWorkflow.LoadAccount)
      step(:capture_payment, IrreversibleWorkflow.CapturePayment, irreversible: true)

      transition(:load_account, on: :ok, to: :capture_payment)
      transition(:capture_payment, on: :ok, to: :complete)
    end
  end

  defmodule IrreversibleWorkflow.LoadAccount do
    use Jido.Action,
      name: "load_account",
      description: "Loads account",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{account: %{id: account_id}}}
    end
  end

  defmodule IrreversibleWorkflow.CapturePayment do
    use Jido.Action,
      name: "capture_payment",
      description: "Captures payment",
      schema: [account: [type: :map, required: true]]

    @impl Jido.Action
    def run(%{account: account}, _context) do
      {:ok, %{payment: %{account_id: account.id, status: "captured"}}}
    end
  end

  defmodule RetryExhaustedWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :check_gateway, RetryExhaustedWorkflow.CheckGateway, retry: [max_attempts: 2]
      transition :check_gateway, on: :ok, to: :complete
    end
  end

  defmodule RetryExhaustedWorkflow.CheckGateway do
    use Jido.Action,
      name: "check_gateway",
      description: "Fails until retries are exhausted",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(_params, _context) do
      {:error, %{message: "gateway timeout", code: "gateway_timeout"}}
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
      description: "Fails before retry",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(_params, _context) do
      {:error, %{message: "gateway timeout", code: "gateway_timeout"}}
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
      step :record_approval, :log, message: "approval recorded", level: :info
      step :record_rejection, :log, message: "rejection recorded", level: :warning

      transition :wait_for_review, on: :ok, to: :record_approval
      transition :wait_for_review, on: :error, to: :record_rejection
      transition :record_approval, on: :ok, to: :complete
      transition :record_rejection, on: :ok, to: :complete
    end
  end

  defmodule DependencyWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :load_account, DependencyWorkflow.LoadAccount
      step :load_invoice, DependencyWorkflow.LoadInvoice
      step :send_email, DependencyWorkflow.SendEmail, after: [:load_account, :load_invoice]
    end
  end

  defmodule DependencyWorkflow.LoadAccount do
    use Jido.Action,
      name: "load_account",
      description: "Loads account",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{account: %{id: account_id}}}
    end
  end

  defmodule DependencyWorkflow.LoadInvoice do
    use Jido.Action,
      name: "load_invoice",
      description: "Loads invoice",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{invoice: %{account_id: account_id}}}
    end
  end

  defmodule DependencyWorkflow.SendEmail do
    use Jido.Action,
      name: "send_email",
      description: "Sends dependency email",
      schema: [
        account: [type: :map, required: true],
        invoice: [type: :map, required: true]
      ]

    @impl Jido.Action
    def run(%{account: account}, _context) do
      {:ok, %{delivery: %{account_id: account.id}}}
    end
  end
end
