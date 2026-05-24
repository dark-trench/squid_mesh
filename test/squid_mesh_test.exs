defmodule SquidMeshTest do
  use SquidMesh.DataCase, async: false

  import ExUnit.CaptureLog

  alias SquidMesh.Executor.Payload
  alias SquidMesh.ReadModel.Explanation.Diagnostic
  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.ReadModel.Listing.Summary
  alias SquidMesh.Run
  alias SquidMesh.Runs
  alias SquidMesh.Runs.StepState
  alias SquidMesh.Runtime.DispatchAgent
  alias SquidMesh.Runtime.DispatchProtocol
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.Journal.Executor
  alias SquidMesh.Runtime.Runner
  alias SquidMesh.Runtime.Unblocker
  alias SquidMesh.Test.Job
  alias SquidMesh.Test.StepWorker
  alias SquidMesh.TestSupport.LazyWorkflow
  alias SquidMesh.Workflow.Definition

  defp execute_journal_next(opts), do: Executor.execute_next(opts)

  defmodule CommitThenFailStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(_key, _opts), do: :not_found

    @impl Jido.Storage
    def put_checkpoint(_key, _data, _opts), do: :ok

    @impl Jido.Storage
    def delete_checkpoint(_key, _opts), do: :ok

    @impl Jido.Storage
    def load_thread("squid_mesh:dispatch:" <> _queue, _opts), do: :not_found
    def load_thread(_thread_id, _opts), do: {:error, :load_failed}

    @impl Jido.Storage
    def append_thread(thread_id, entries, _opts) do
      thread =
        [id: thread_id]
        |> Jido.Thread.new()
        |> Jido.Thread.append(entries)

      {:ok, thread}
    end

    @impl Jido.Storage
    def delete_thread(_thread_id, _opts), do: :ok
  end

  defmodule InvoiceReminderWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :invoice_delivery do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_invoice, InvoiceReminderWorkflow.LoadInvoice
      step :send_email, InvoiceReminderWorkflow.SendEmail, retry: [max_attempts: 3]

      transition :load_invoice, on: :ok, to: :send_email
      transition :send_email, on: :ok, to: :complete
    end
  end

  defmodule PaymentRecoveryWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :gateway_recovery do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :check_gateway, PaymentRecoveryWorkflow.CheckGateway, retry: [max_attempts: 2]
      transition :check_gateway, on: :ok, to: :complete
    end
  end

  defmodule JournalConditionalWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :conditional_route do
        manual()

        payload do
          field :account_id, :string
          field :decision, :string
        end
      end

      step :classify, JournalConditionalWorkflow.Classify
      step :auto_approve, JournalConditionalWorkflow.AutoApprove
      step :manual_review, JournalConditionalWorkflow.ManualReview

      transition :classify,
        on: :ok,
        to: :auto_approve,
        condition: [path: [:routing, :decision], equals: "auto"]

      transition :classify, on: :ok, to: :manual_review
      transition :auto_approve, on: :ok, to: :complete
      transition :manual_review, on: :ok, to: :complete
    end
  end

  defmodule JournalAccumulatedConditionalWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :accumulated_conditional_route do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :load_profile, JournalAccumulatedConditionalWorkflow.LoadProfile

      step :classify, JournalAccumulatedConditionalWorkflow.Classify,
        input: [account_id: [:account_id]]

      step :auto_approve, JournalAccumulatedConditionalWorkflow.AutoApprove
      step :manual_review, JournalAccumulatedConditionalWorkflow.ManualReview

      transition :load_profile, on: :ok, to: :classify

      transition :classify,
        on: :ok,
        to: :auto_approve,
        condition: [path: [:profile, :tier], equals: "trusted"]

      transition :classify, on: :ok, to: :manual_review
      transition :auto_approve, on: :ok, to: :complete
      transition :manual_review, on: :ok, to: :complete
    end
  end

  defmodule JournalFailureWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_failure do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :fail_gateway, JournalFailureWorkflow.FailGateway
      transition :fail_gateway, on: :ok, to: :complete
    end
  end

  defmodule JournalErrorTransitionWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_error_transition do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :fail_gateway, JournalErrorTransitionWorkflow.FailGateway
      step :notify_failure, JournalErrorTransitionWorkflow.NotifyFailure

      transition :fail_gateway, on: :ok, to: :complete
      transition :fail_gateway, on: :error, to: :notify_failure
      transition :notify_failure, on: :ok, to: :complete
    end
  end

  defmodule JournalConditionalErrorCompleteWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_error_complete do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :fail_gateway, JournalConditionalErrorCompleteWorkflow.FailGateway

      transition :fail_gateway, on: :ok, to: :complete

      transition :fail_gateway,
        on: :error,
        to: :complete,
        condition: [path: [:account_id], equals: "acct_123"]
    end
  end

  defmodule JournalRetryWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_retry do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :retry_gateway, JournalRetryWorkflow.RetryGateway, retry: [max_attempts: 2]
      transition :retry_gateway, on: :ok, to: :complete
    end
  end

  defmodule JournalSecretFailureWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_secret_failure do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :leak_secret, JournalSecretFailureWorkflow.LeakSecret
      transition :leak_secret, on: :ok, to: :complete
    end
  end

  defmodule JournalConflictWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_conflict do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :write_conflict, JournalConflictWorkflow.WriteConflict
      transition :write_conflict, on: :ok, to: :complete
    end
  end

  defmodule JournalDependencyWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_dependency do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, JournalDependencyWorkflow.LoadAccount
      step :load_invoice, JournalDependencyWorkflow.LoadInvoice
      step :send_email, JournalDependencyWorkflow.SendEmail, after: [:load_account, :load_invoice]
    end
  end

  defmodule JournalDependencyWaitWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_dependency_wait do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, JournalDependencyWorkflow.LoadAccount
      step :wait_for_settlement, :wait, duration: 2_000, after: [:load_account]
      step :load_invoice, JournalDependencyWorkflow.LoadInvoice

      step :send_email, JournalDependencyWorkflow.SendEmail,
        after: [:wait_for_settlement, :load_invoice]
    end
  end

  defmodule JournalRootWaitWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_root_wait do
        manual()

        payload do
          field :invoice_id, :string
        end
      end

      step :wait_for_settlement, :wait, duration: 2_000
      step :z_load_invoice, JournalDependencyWorkflow.LoadInvoice

      step :send_email, JournalRootWaitWorkflow.SendEmail,
        after: [:wait_for_settlement, :z_load_invoice]
    end
  end

  defmodule JournalDependencyFailureWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_dependency_failure do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, JournalDependencyFailureWorkflow.LoadAccount
      step :load_invoice, JournalDependencyFailureWorkflow.LoadInvoice

      step :send_email, JournalDependencyFailureWorkflow.SendEmail,
        after: [:load_account, :load_invoice]
    end
  end

  defmodule JournalMissingPathWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_missing_path do
        manual()

        payload do
          field :draft, :map
        end
      end

      step :load_review_context, JournalMissingPathWorkflow.LoadReviewContext

      step :record_review, JournalMissingPathWorkflow.RecordReview,
        input: [drafts: [:draft, :drafts]]

      transition :load_review_context, on: :ok, to: :record_review
      transition :record_review, on: :ok, to: :complete
    end
  end

  defmodule ReorderedWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :invoice_delivery do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :send_email, ReorderedWorkflow.SendEmail
      step :load_invoice, ReorderedWorkflow.LoadInvoice

      transition :load_invoice, on: :ok, to: :send_email
      transition :send_email, on: :ok, to: :complete
    end
  end

  defmodule IrreversibleWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :payment_capture do
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

  defmodule InvoiceReminderWorkflow.LoadInvoice do
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

  defmodule InvoiceReminderWorkflow.SendEmail do
    use Jido.Action,
      name: "send_email",
      description: "Sends an invoice reminder email",
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

  defmodule PaymentRecoveryWorkflow.CheckGateway do
    use Jido.Action,
      name: "check_gateway",
      description: "Checks payment gateway status",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{gateway_check: %{account_id: account_id, status: "healthy"}}}
    end
  end

  defmodule JournalConditionalWorkflow.Classify do
    use Jido.Action,
      name: "classify",
      description: "Classifies a conditional journal route",
      schema: [
        account_id: [type: :string, required: true],
        decision: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{decision: decision}, _context) do
      {:ok, %{routing: %{decision: decision}}}
    end
  end

  defmodule JournalConditionalWorkflow.AutoApprove do
    use Jido.Action,
      name: "auto_approve",
      description: "Records automatic approval",
      schema: [routing: [type: :map, required: true]]

    @impl Jido.Action
    def run(_input, _context), do: {:ok, %{approval: %{mode: "auto"}}}
  end

  defmodule JournalConditionalWorkflow.ManualReview do
    use Jido.Action,
      name: "manual_review",
      description: "Records manual review",
      schema: [routing: [type: :map, required: true]]

    @impl Jido.Action
    def run(_input, _context), do: {:ok, %{approval: %{mode: "manual"}}}
  end

  defmodule JournalAccumulatedConditionalWorkflow.LoadProfile do
    use Jido.Action,
      name: "load_profile",
      description: "Loads account profile data used by a later branch",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{profile: %{account_id: account_id, tier: "trusted"}}}
    end
  end

  defmodule JournalAccumulatedConditionalWorkflow.Classify do
    use Jido.Action,
      name: "classify_accumulated",
      description: "Returns no profile data so routing must use accumulated context",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(_input, _context), do: {:ok, %{routing: %{checked?: true}}}
  end

  defmodule JournalAccumulatedConditionalWorkflow.AutoApprove do
    use Jido.Action,
      name: "auto_approve_accumulated",
      description: "Records automatic approval",
      schema: [profile: [type: :map, required: true]]

    @impl Jido.Action
    def run(_input, _context), do: {:ok, %{approval: %{mode: "auto"}}}
  end

  defmodule JournalAccumulatedConditionalWorkflow.ManualReview do
    use Jido.Action,
      name: "manual_review_accumulated",
      description: "Records manual review",
      schema: [routing: [type: :map, required: true]]

    @impl Jido.Action
    def run(_input, _context), do: {:ok, %{approval: %{mode: "manual"}}}
  end

  defmodule JournalFailureWorkflow.FailGateway do
    use Jido.Action,
      name: "fail_gateway",
      description: "Fails payment gateway status checks",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:error,
       %{
         code: "gateway_timeout",
         message: "gateway timeout",
         retryable?: false,
         account_id: account_id
       }}
    end
  end

  defmodule JournalErrorTransitionWorkflow.FailGateway do
    use Jido.Action,
      name: "fail_gateway_for_error_transition",
      description: "Fails nonretryably so the workflow follows its error transition",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, context) do
      if hook = :persistent_term.get(:journal_error_transition_conflict_hook, nil) do
        hook.(context)
      end

      {:error,
       %{
         code: "gateway_timeout",
         message: "gateway timeout",
         retryable?: false,
         account_id: account_id
       }}
    end
  end

  defmodule JournalConditionalErrorCompleteWorkflow.FailGateway do
    use Jido.Action,
      name: "fail_gateway",
      description: "Fails and routes to terminal completion",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(_params, context) do
      if hook = :persistent_term.get(:journal_conditional_error_complete_conflict_hook, nil) do
        hook.(context)
      end

      {:error, %{message: "gateway timeout", code: "gateway_timeout", retryable?: false}}
    end
  end

  defmodule JournalErrorTransitionWorkflow.NotifyFailure do
    use Jido.Action,
      name: "notify_failure_for_error_transition",
      description: "Records that the error transition ran",
      schema: []

    @impl Jido.Action
    def run(_params, _context) do
      {:ok, %{failure_notification: %{channel: "email"}}}
    end
  end

  defmodule JournalRetryWorkflow.RetryGateway do
    use Jido.Action,
      name: "retry_gateway",
      description: "Fails retryably",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, context) do
      if hook = :persistent_term.get(:journal_retry_failure_conflict_hook, nil) do
        hook.(context)
      end

      {:error,
       %{
         code: "gateway_timeout",
         message: "gateway timeout",
         retryable?: true,
         account_id: account_id
       }}
    end
  end

  defmodule JournalSecretFailureWorkflow.LeakSecret do
    use Jido.Action,
      name: "leak_secret",
      description: "Returns a secret-bearing error",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(_params, _context) do
      {:error,
       %{
         code: "token=super-secret-token",
         message: "token=super-secret-token",
         retryable?: false,
         validation_errors: %{authorization: "Bearer super-secret-token"}
       }}
    end
  end

  defmodule JournalConflictWorkflow.WriteConflict do
    use Jido.Action,
      name: "write_conflict",
      description: "Runs a test hook before returning",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      if hook = :persistent_term.get(:journal_executor_conflict_hook, nil) do
        hook.()
      end

      {:ok, %{conflict_probe: %{account_id: account_id, status: "written"}}}
    end
  end

  defmodule JournalDependencyWorkflow.LoadAccount do
    use Jido.Action,
      name: "journal_load_account",
      description: "Loads account for journal dependency workflow",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{account: %{id: account_id}}}
    end
  end

  defmodule JournalDependencyWorkflow.LoadInvoice do
    use Jido.Action,
      name: "journal_load_invoice",
      description: "Loads invoice for journal dependency workflow",
      schema: [invoice_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{invoice_id: invoice_id}, _context) do
      if hook = :persistent_term.get(:journal_dependency_invoice_hook, nil) do
        hook.()
      end

      {:ok, %{invoice: %{id: invoice_id, status: "open"}}}
    end
  end

  defmodule JournalDependencyWorkflow.SendEmail do
    use Jido.Action,
      name: "journal_send_dependency_email",
      description: "Sends dependency email",
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

  defmodule JournalRootWaitWorkflow.SendEmail do
    use Jido.Action,
      name: "journal_send_root_wait_email",
      description: "Sends dependency email after a root wait",
      schema: [
        invoice: [type: :map, required: true]
      ]

    @impl Jido.Action
    def run(%{invoice: invoice}, _context) do
      {:ok, %{delivery: %{invoice_id: invoice.id, channel: "email"}}}
    end
  end

  defmodule JournalDependencyFailureWorkflow.LoadAccount do
    use Jido.Action,
      name: "journal_dependency_fail_account",
      description: "Fails account loading for journal dependency workflow",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:error,
       %{
         code: "account_unavailable",
         message: "account unavailable",
         retryable?: false,
         account_id: account_id
       }}
    end
  end

  defmodule JournalDependencyFailureWorkflow.LoadInvoice do
    defdelegate run(params, context), to: JournalDependencyWorkflow.LoadInvoice
  end

  defmodule JournalDependencyFailureWorkflow.SendEmail do
    defdelegate run(params, context), to: JournalDependencyWorkflow.SendEmail
  end

  defmodule JournalMissingPathWorkflow.LoadReviewContext do
    use Jido.Action,
      name: "journal_missing_path_load_context",
      description: "Returns a partial nested context",
      schema: [draft: [type: :map, required: true]]

    @impl Jido.Action
    def run(%{draft: draft}, _context), do: {:ok, %{draft: draft}}
  end

  defmodule JournalMissingPathWorkflow.RecordReview do
    use Jido.Action,
      name: "journal_missing_path_record_review",
      description: "Should not execute when successor mapped input is missing",
      schema: [drafts: [type: {:list, :map}, required: true]]

    @impl Jido.Action
    def run(_params, _context) do
      raise "record_review should not execute when successor mapped input is missing"
    end
  end

  defmodule ReorderedWorkflow.LoadInvoice do
    defdelegate run(params, context), to: InvoiceReminderWorkflow.LoadInvoice
  end

  defmodule IrreversibleWorkflow.LoadAccount do
    use Jido.Action,
      name: "load_account",
      description: "Loads account details",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{account: %{id: account_id}}}
    end
  end

  defmodule IrreversibleWorkflow.CapturePayment do
    use Jido.Action,
      name: "capture_payment",
      description: "Captures a payment",
      schema: [account: [type: :map, required: true]]

    @impl Jido.Action
    def run(%{account: account}, _context) do
      {:ok, %{payment: %{account_id: account.id, status: "captured"}}}
    end
  end

  defmodule ReorderedWorkflow.SendEmail do
    defdelegate run(params, context), to: InvoiceReminderWorkflow.SendEmail
  end

  defmodule WorkflowWithPayloadDefaults do
    use SquidMesh.Workflow

    workflow do
      trigger :invoice_delivery do
        manual()

        payload do
          field :team_id, :string, default: "backend"
          field :prompt_date, :string, default: {:today, :iso8601}
          field :invoice_id, :string
        end
      end

      step :deliver_invoice, WorkflowWithPayloadDefaults.DeliverInvoice
      transition :deliver_invoice, on: :ok, to: :complete
    end
  end

  defmodule DailyStandupWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :daily_standup do
        cron "@reboot", timezone: "Etc/UTC"

        payload do
          field :team_id, :string, default: "backend"
          field :prompt_date, :string, default: {:today, :iso8601}
        end
      end

      step :announce_prompt, :log, message: "posting daily standup"
      transition :announce_prompt, on: :ok, to: :complete
    end
  end

  defmodule ManualAndScheduledDigestWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual_digest do
        manual()

        payload do
          field :chat_id, :integer
        end
      end

      trigger :scheduled_digest do
        cron "@reboot", timezone: "Etc/UTC"

        payload do
          field :window_start_at, :string, default: {:today, :iso8601}
        end
      end

      step :announce_prompt, :log, message: "posting digest", level: :warning
      transition :announce_prompt, on: :ok, to: :complete
    end
  end

  defmodule ScheduledContextWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :scheduled_capture do
        cron "@hourly", timezone: "Etc/UTC"
      end

      step :capture_schedule, ScheduledContextWorkflow.CaptureSchedule
      transition :capture_schedule, on: :ok, to: :complete
    end
  end

  defmodule AnotherScheduledContextWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :scheduled_capture do
        cron "@hourly", timezone: "Etc/UTC"
      end

      step :capture_schedule, ScheduledContextWorkflow.CaptureSchedule
      transition :capture_schedule, on: :ok, to: :complete
    end
  end

  defmodule ScheduledContextWorkflow.CaptureSchedule do
    use SquidMesh.Step,
      name: :capture_schedule,
      output_schema: [schedule_seen: [type: :map, required: true]]

    @impl SquidMesh.Step
    def run(_input, context) do
      {:ok, %{schedule_seen: Map.fetch!(context.state, :schedule)}}
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

      step :record_delivery, :log,
        message: "delivery recorded",
        level: :info,
        input: [account_id: [:account_id]]

      transition :wait_for_approval, on: :ok, to: :record_delivery
      transition :record_delivery, on: :ok, to: :complete
    end
  end

  defmodule WaitWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :wait_for_settlement, :wait, duration: 2_000
      step :record_settlement, :log, message: "settlement recorded", level: :info

      transition :wait_for_settlement, on: :ok, to: :record_settlement
      transition :record_settlement, on: :ok, to: :complete
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

  defmodule IncompleteExecutor do
    def enqueue_step(_config, _run, _step, _opts), do: {:ok, %{}}
  end

  defp use_runtime_tables(_context) do
    preserved_config =
      for key <- [:runtime, :read_model, :journal_storage], into: %{} do
        {key, Application.fetch_env(:squid_mesh, key)}
      end

    Application.put_env(:squid_mesh, :runtime, :runtime_tables)
    Application.put_env(:squid_mesh, :read_model, :runtime_tables)
    Application.delete_env(:squid_mesh, :journal_storage)

    on_exit(fn ->
      Enum.each(preserved_config, fn
        {key, {:ok, value}} -> Application.put_env(:squid_mesh, key, value)
        {key, :error} -> Application.delete_env(:squid_mesh, key)
      end)
    end)

    :ok
  end

  defp without_squid_mesh_env(keys, fun) when is_list(keys) and is_function(fun, 0) do
    preserved_config =
      for key <- keys, into: %{} do
        {key, Application.fetch_env(:squid_mesh, key)}
      end

    Enum.each(keys, &Application.delete_env(:squid_mesh, &1))

    try do
      fun.()
    after
      Enum.each(preserved_config, fn
        {key, {:ok, value}} -> Application.put_env(:squid_mesh, key, value)
        {key, :error} -> Application.delete_env(:squid_mesh, key)
      end)
    end
  end

  test "configures an application supervisor" do
    assert Application.spec(:squid_mesh, :mod) == {SquidMesh.Application, []}
  end

  test "loads the public entrypoint module" do
    assert Code.ensure_loaded?(SquidMesh)
  end

  describe "config/1" do
    test "returns the validated host app contract with defaults" do
      assert {:ok, config} =
               SquidMesh.config(repo: SquidMesh.Test.Repo, executor: SquidMesh.Test.Executor)

      assert config.repo == SquidMesh.Test.Repo
      assert config.executor == SquidMesh.Test.Executor
      assert config.stale_step_timeout == :disabled
      assert config.runtime == :journal
      assert config.read_model == :read_model
      assert config.journal_storage.adapter == SquidMesh.Runtime.Journal.Storage.Ecto
      assert config.journal_storage.opts == [repo: SquidMesh.Test.Repo]
      assert config.queue == "default"
    end

    test "journal defaults do not require a host executor" do
      without_squid_mesh_env([:executor], fn ->
        assert {:ok, config} = SquidMesh.config(repo: SquidMesh.Test.Repo)

        assert config.repo == SquidMesh.Test.Repo
        assert config.executor == nil
        assert config.runtime == :journal
        assert config.read_model == :read_model
        assert config.journal_storage.adapter == SquidMesh.Runtime.Journal.Storage.Ecto
        assert config.journal_storage.opts == [repo: SquidMesh.Test.Repo]
      end)
    end

    test "allows host applications to configure stale step timeout" do
      overrides = [
        repo: SquidMesh.Test.Repo,
        executor: SquidMesh.Test.Executor,
        stale_step_timeout: 60_000
      ]

      assert {:ok, config} = SquidMesh.config(overrides)

      assert config.stale_step_timeout == 60_000
    end

    test "allows host applications to configure journal runtime defaults" do
      journal_storage = {Jido.Storage.ETS, table: :squid_mesh_config_test}

      overrides = [
        repo: SquidMesh.Test.Repo,
        executor: SquidMesh.Test.Executor,
        runtime: :journal,
        read_model: :read_model,
        journal_storage: journal_storage,
        queue: :configured_queue
      ]

      assert {:ok, config} = SquidMesh.config(overrides)

      assert config.runtime == :journal
      assert config.read_model == :read_model
      assert config.journal_storage.adapter == Jido.Storage.ETS
      assert config.journal_storage.opts == [table: :squid_mesh_config_test]
      assert config.queue == "configured_queue"
    end

    test "infers Ecto journal storage from the configured repo when runtime uses the journal" do
      required = [
        repo: SquidMesh.Test.Repo,
        executor: SquidMesh.Test.Executor
      ]

      assert {:ok, config} = SquidMesh.config(Keyword.put(required, :runtime, :journal))

      assert config.runtime == :journal
      assert config.journal_storage.adapter == SquidMesh.Runtime.Journal.Storage.Ecto
      assert config.journal_storage.opts == [repo: SquidMesh.Test.Repo]
    end

    test "infers Ecto journal storage from the configured repo when read model uses the journal" do
      required = [
        repo: SquidMesh.Test.Repo,
        executor: SquidMesh.Test.Executor
      ]

      assert {:ok, config} = SquidMesh.config(Keyword.put(required, :read_model, :read_model))

      assert config.read_model == :read_model
      assert config.journal_storage.adapter == SquidMesh.Runtime.Journal.Storage.Ecto
      assert config.journal_storage.opts == [repo: SquidMesh.Test.Repo]
    end

    test "rejects explicit nil journal storage when configured runtime or read model uses the journal" do
      required = [
        repo: SquidMesh.Test.Repo,
        executor: SquidMesh.Test.Executor,
        journal_storage: nil
      ]

      assert {:error, {:missing_config, [:journal_storage]}} =
               SquidMesh.config(Keyword.put(required, :runtime, :journal))

      assert {:error, {:missing_config, [:journal_storage]}} =
               SquidMesh.config(Keyword.put(required, :read_model, :read_model))
    end

    test "runtime-table configuration ignores journal storage settings" do
      assert {:ok, config} =
               SquidMesh.config(
                 repo: SquidMesh.Test.Repo,
                 executor: SquidMesh.Test.Executor,
                 runtime: :runtime_tables,
                 read_model: :runtime_tables,
                 journal_storage: nil
               )

      assert config.runtime == :runtime_tables
      assert config.read_model == :runtime_tables
      assert config.journal_storage == nil

      assert {:ok, config} =
               SquidMesh.config(
                 repo: SquidMesh.Test.Repo,
                 executor: SquidMesh.Test.Executor,
                 runtime: :runtime_tables,
                 read_model: :runtime_tables,
                 journal_storage: :not_used_by_runtime_tables
               )

      assert config.journal_storage == nil
    end

    test "redacts invalid queue settings in config errors" do
      secret_queue = %{claim_token: "super-secret-token"}

      assert {:error, {:invalid_config, [queue: :invalid]} = reason} =
               SquidMesh.config(
                 repo: SquidMesh.Test.Repo,
                 executor: SquidMesh.Test.Executor,
                 queue: secret_queue
               )

      refute inspect(reason) =~ "super-secret-token"

      assert_raise ArgumentError, ~r/queue=:invalid/, fn ->
        SquidMesh.config!(
          repo: SquidMesh.Test.Repo,
          executor: SquidMesh.Test.Executor,
          queue: secret_queue
        )
      end
    end

    test "reports missing required configuration keys" do
      original_repo = Application.get_env(:squid_mesh, :repo)
      original_executor = Application.get_env(:squid_mesh, :executor)

      on_exit(fn ->
        Application.put_env(:squid_mesh, :repo, original_repo)
        Application.put_env(:squid_mesh, :executor, original_executor)
      end)

      Application.delete_env(:squid_mesh, :repo)
      Application.delete_env(:squid_mesh, :executor)

      assert {:error, {:missing_config, [:repo]}} = SquidMesh.config()
    end

    test "runtime-table configuration requires a host executor" do
      without_squid_mesh_env([:executor], fn ->
        assert {:error, {:missing_config, [:executor]}} =
                 SquidMesh.config(repo: SquidMesh.Test.Repo, runtime: :runtime_tables)
      end)
    end

    test "reports executor modules missing required callbacks" do
      assert {:error, {:invalid_config, [executor: {:missing_callbacks, missing}]}} =
               SquidMesh.config(repo: SquidMesh.Test.Repo, executor: IncompleteExecutor)

      assert :enqueue_steps in missing
      assert :enqueue_compensation in missing
      assert :enqueue_cron in missing
    end

    test "reports unloadable executor modules" do
      assert {:error,
              {:invalid_config, [executor: {:module_not_loaded, SquidMeshTest.UnknownExecutor}]}} =
               SquidMesh.config(
                 repo: SquidMesh.Test.Repo,
                 executor: SquidMeshTest.UnknownExecutor
               )
    end

    test "reports invalid stale step timeout settings" do
      assert {:error, {:invalid_config, [stale_step_timeout: -1]}} =
               SquidMesh.config(
                 repo: SquidMesh.Test.Repo,
                 executor: SquidMesh.Test.Executor,
                 stale_step_timeout: -1
               )
    end
  end

  describe "journal default unsupported table-runtime operations" do
    test "list_runs/2 returns an empty journal catalog when no runs exist" do
      assert {:ok, []} =
               SquidMesh.list_runs([], repo: Repo)
    end

    test "cancel_run/2 returns an explicit unsupported runtime error" do
      assert {:error, {:unsupported_runtime, {:journal, :cancel_run}}} =
               SquidMesh.cancel_run(Ecto.UUID.generate(), repo: Repo)
    end

    test "replay_run/2 returns an explicit unsupported runtime error" do
      assert {:error, {:unsupported_runtime, {:journal, :replay_run}}} =
               SquidMesh.replay_run(Ecto.UUID.generate(), repo: Repo)
    end

    test "cron starts return an explicit unsupported runtime error" do
      assert {:error, {:unsupported_runtime, {:journal, :start_run_with_initial_context}}} =
               SquidMesh.start_run_with_initial_context(
                 ScheduledCaptureWorkflow,
                 :scheduled_capture,
                 %{},
                 %{schedule: %{idempotency_key: "journal-default-unsupported-cron"}},
                 repo: Repo
               )
    end
  end

  describe "start_run/3" do
    setup :use_runtime_tables

    test "persists a new run and returns the public run shape" do
      payload = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:ok, %Run{} = run} =
               SquidMesh.start_run(InvoiceReminderWorkflow, payload, repo: Repo)

      assert run.workflow == InvoiceReminderWorkflow
      assert run.trigger == :invoice_delivery
      assert run.status == :pending
      assert run.payload == payload
      assert run.context == %{}
      assert run.current_step == :load_invoice
      assert run.last_error == nil
      assert is_binary(run.id)
      assert %DateTime{} = run.inserted_at
      assert %DateTime{} = run.updated_at
    end

    test "rejects caller-supplied run context through public start options" do
      payload = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:error, {:invalid_option, :context}} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 payload,
                 repo: Repo,
                 context: %{schedule: %{signal_id: "fake"}}
               )
    end

    test "rejects internal initial context through public start options" do
      payload = %{account_id: "acct_123", invoice_id: "inv_456"}

      assert {:error, {:invalid_option, :initial_context}} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 payload,
                 repo: Repo,
                 initial_context: %{schedule: %{signal_id: "fake"}}
               )
    end

    test "rejects modules that do not define the workflow contract" do
      assert {:error, {:invalid_workflow, String}} =
               SquidMesh.start_run(String, %{}, repo: Repo)
    end

    test "loads workflow modules on demand before validating the contract" do
      :code.purge(LazyWorkflow)
      :code.delete(LazyWorkflow)

      refute :code.is_loaded(LazyWorkflow)

      assert {:ok, %Run{} = run} =
               SquidMesh.start_run(LazyWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert run.workflow == LazyWorkflow
    end

    test "starts a run through an explicit trigger name" do
      assert {:ok, %Run{} = run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 :invoice_delivery,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 repo: Repo
               )

      assert run.trigger == :invoice_delivery
    end

    test "rejects unknown trigger names" do
      assert {:error, {:invalid_trigger, :unknown_trigger}} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 :unknown_trigger,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 repo: Repo
               )
    end

    test "rejects non-map payloads" do
      assert {:error, {:invalid_payload, :expected_map}} =
               SquidMesh.start_run(InvoiceReminderWorkflow, [:not_a_map], repo: Repo)
    end

    test "starts from the semantic entry step rather than declaration order" do
      assert {:ok, run} =
               SquidMesh.start_run(ReorderedWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert run.current_step == :load_invoice
    end

    test "rejects payloads with missing required fields" do
      assert {:error, {:invalid_payload, %{missing_fields: [:invoice_id]}}} =
               SquidMesh.start_run(InvoiceReminderWorkflow, %{account_id: "acct_123"}, repo: Repo)
    end

    test "rejects payloads with undeclared fields" do
      assert {:error, {:invalid_payload, %{unknown_fields: [:unexpected]}}} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456", unexpected: true},
                 repo: Repo
               )
    end

    test "rejects payload fields with invalid types" do
      assert {:error, {:invalid_payload, %{invalid_types: %{invoice_id: :string}}}} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: 123},
                 repo: Repo
               )
    end

    test "applies payload defaults before persistence" do
      assert {:ok, %Run{} = run} =
               SquidMesh.start_run(
                 WorkflowWithPayloadDefaults,
                 %{invoice_id: "inv_456"},
                 repo: Repo
               )

      assert run.payload == %{
               team_id: "backend",
               prompt_date: Date.to_iso8601(Date.utc_today()),
               invoice_id: "inv_456"
             }
    end

    test "allows provided payload values to override defaults" do
      assert {:ok, %Run{} = run} =
               SquidMesh.start_run(
                 WorkflowWithPayloadDefaults,
                 %{
                   invoice_id: "inv_456",
                   team_id: "payments",
                   prompt_date: "2026-01-15"
                 },
                 repo: Repo
               )

      assert run.payload == %{
               team_id: "payments",
               prompt_date: "2026-01-15",
               invoice_id: "inv_456"
             }
    end

    test "rolls back run creation when dispatching the first step fails" do
      before_count = Repo.aggregate(SquidMesh.Persistence.Run, :count, :id)
      SquidMesh.Test.Executor.fail_next!()

      assert {:error, {:dispatch_failed, :executor_unavailable}} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 repo: Repo
               )

      assert Repo.aggregate(SquidMesh.Persistence.Run, :count, :id) == before_count
    end
  end

  describe "cron trigger activation" do
    setup :use_runtime_tables

    test "starts a cron workflow run from the neutral runner" do
      assert :ok =
               Runner.start_cron_trigger(
                 "Elixir.SquidMeshTest.DailyStandupWorkflow",
                 "daily_standup",
                 repo: Repo
               )

      assert_enqueued(
        worker: SquidMesh.Test.StepWorker,
        queue: "squid_mesh",
        args: %{"step" => "announce_prompt"}
      )

      assert [%SquidMesh.Persistence.Run{} = persisted_run] = Repo.all(SquidMesh.Persistence.Run)

      assert persisted_run.workflow == "Elixir.SquidMeshTest.DailyStandupWorkflow"
      assert persisted_run.trigger == "daily_standup"
      assert persisted_run.input["team_id"] == "backend"
      assert is_binary(persisted_run.input["prompt_date"])
    end

    test "starts a selected cron trigger from a multi-trigger workflow" do
      assert :ok =
               Runner.start_cron_trigger(
                 "Elixir.SquidMeshTest.ManualAndScheduledDigestWorkflow",
                 "scheduled_digest",
                 repo: Repo
               )

      assert [%SquidMesh.Persistence.Run{} = persisted_run] = Repo.all(SquidMesh.Persistence.Run)

      assert persisted_run.workflow ==
               "Elixir.SquidMeshTest.ManualAndScheduledDigestWorkflow"

      assert persisted_run.trigger == "scheduled_digest"
      assert is_binary(persisted_run.input["window_start_at"])
      refute Map.has_key?(persisted_run.input, "chat_id")
    end

    test "persists explicit scheduled signal id before workflow step execution" do
      payload =
        Payload.cron(
          ScheduledContextWorkflow,
          :scheduled_capture,
          signal_id: "signal_123",
          intended_window: %{
            start_at: "2026-05-15T09:00:00Z",
            end_at: "2026-05-15T10:00:00Z"
          }
        )

      assert :ok = Runner.perform(payload, repo: Repo)

      assert [%SquidMesh.Persistence.Run{} = persisted_run] = Repo.all(SquidMesh.Persistence.Run)

      assert persisted_run.context["schedule"]["signal_id"] == "signal_123"
      refute String.starts_with?(persisted_run.context["schedule"]["signal_id"], "sha256:")
      assert persisted_run.context["schedule"]["trigger_name"] == "scheduled_capture"
      assert persisted_run.context["schedule"]["cron_expression"] == "@hourly"
      assert persisted_run.context["schedule"]["timezone"] == "Etc/UTC"

      assert persisted_run.context["schedule"]["intended_window"] == %{
               "start_at" => "2026-05-15T09:00:00Z",
               "end_at" => "2026-05-15T10:00:00Z"
             }

      received_at = persisted_run.context["schedule"]["received_at"]
      assert is_binary(received_at)
      refute received_at == "2026-05-15T09:00:00Z"
      assert {:ok, _received_at, 0} = DateTime.from_iso8601(received_at)

      assert {:ok, inspected_run} = SquidMesh.inspect_run(persisted_run.id, repo: Repo)

      assert inspected_run.context.schedule.intended_window.start_at ==
               "2026-05-15T09:00:00Z"

      assert {:ok, explanation} = SquidMesh.explain_run(persisted_run.id, repo: Repo)

      assert explanation.evidence.run.schedule.signal_id == "signal_123"
      assert explanation.evidence.run.schedule.trigger_name == "scheduled_capture"

      assert %{success: 1, failure: 0} = SquidMesh.Test.Executor.drain()

      assert {:ok, completed_run} = SquidMesh.inspect_run(persisted_run.id, repo: Repo)
      assert completed_run.context.schedule_seen == completed_run.context.schedule
    end

    test "derives stable signal ids from intended schedule windows" do
      payload =
        Payload.cron(
          ScheduledContextWorkflow,
          :scheduled_capture,
          intended_window: %{
            start_at: "2026-05-15T09:00:00Z",
            end_at: "2026-05-15T10:00:00Z"
          }
        )

      assert :ok = Runner.perform(payload, repo: Repo)
      assert :ok = Runner.perform(payload, repo: Repo)

      assert [%SquidMesh.Persistence.Run{}, %SquidMesh.Persistence.Run{}] =
               persisted_runs = Repo.all(SquidMesh.Persistence.Run)

      signal_ids =
        Enum.map(persisted_runs, fn persisted_run ->
          persisted_run.context["schedule"]["signal_id"]
        end)

      assert Enum.uniq(signal_ids) == [hd(signal_ids)]

      assert hd(signal_ids) =~
               ~r/^sha256:[A-Za-z0-9_-]{43}$/
    end

    test "omits derived signal ids when schedule windows are incomplete" do
      payload =
        Payload.cron(
          ScheduledContextWorkflow,
          :scheduled_capture,
          intended_window: %{
            start_at: "2026-05-15T09:00:00Z"
          }
        )

      assert :ok = Runner.perform(payload, repo: Repo)

      assert [%SquidMesh.Persistence.Run{} = persisted_run] = Repo.all(SquidMesh.Persistence.Run)

      refute Map.has_key?(persisted_run.context["schedule"], "signal_id")
    end

    test "scopes derived signal ids by workflow" do
      intended_window = %{
        start_at: "2026-05-15T09:00:00Z",
        end_at: "2026-05-15T10:00:00Z"
      }

      payload =
        Payload.cron(
          ScheduledContextWorkflow,
          :scheduled_capture,
          intended_window: intended_window
        )

      other_payload =
        Payload.cron(
          AnotherScheduledContextWorkflow,
          :scheduled_capture,
          intended_window: intended_window
        )

      assert :ok = Runner.perform(payload, repo: Repo)
      assert :ok = Runner.perform(other_payload, repo: Repo)

      signal_ids =
        SquidMesh.Persistence.Run
        |> Repo.all()
        |> Enum.map(fn persisted_run -> persisted_run.context["schedule"]["signal_id"] end)

      assert length(Enum.uniq(signal_ids)) == 2
    end

    test "rejects malformed scheduler signal ids" do
      payload =
        ScheduledContextWorkflow
        |> Payload.cron(:scheduled_capture)
        |> Map.put("signal_id", 123)

      assert {:error, {:invalid_schedule_signal_id, 123}} =
               Runner.perform(payload, repo: Repo)

      assert [] = Repo.all(SquidMesh.Persistence.Run)
    end

    test "preserves atom-keyed scheduler metadata from delivered cron payloads" do
      payload =
        ScheduledContextWorkflow
        |> Payload.cron(:scheduled_capture)
        |> Map.put(:signal_id, "signal_123")
        |> Map.put(:intended_window, %{
          start_at: "2026-05-15T09:00:00Z",
          end_at: "2026-05-15T10:00:00Z"
        })

      assert :ok = Runner.perform(payload, repo: Repo)

      assert [%SquidMesh.Persistence.Run{} = persisted_run] = Repo.all(SquidMesh.Persistence.Run)

      assert persisted_run.context["schedule"]["signal_id"] == "signal_123"

      assert persisted_run.context["schedule"]["intended_window"] == %{
               "start_at" => "2026-05-15T09:00:00Z",
               "end_at" => "2026-05-15T10:00:00Z"
             }
    end
  end

  describe "inspect_run/2" do
    setup :use_runtime_tables

    test "fetches a persisted run by id" do
      assert {:ok, created_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      assert {:ok, %Run{} = inspected_run} = SquidMesh.inspect_run(created_run.id, repo: Repo)

      assert inspected_run == created_run
    end

    test "runtime-table read model ignores projection-only options" do
      assert {:ok, created_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      assert {:ok, %Run{} = inspected_run} =
               SquidMesh.inspect_run(created_run.id,
                 read_model: :runtime_tables,
                 journal_storage: :not_used_by_runtime_tables,
                 queue: "default",
                 now: ~U[2026-05-15 00:00:00Z],
                 repo: Repo
               )

      assert inspected_run.id == created_run.id
    end

    test "returns not found when the run does not exist" do
      assert {:error, :not_found} =
               SquidMesh.inspect_run(Ecto.UUID.generate(), repo: Repo)
    end

    test "returns a structured error for malformed run ids" do
      assert {:error, :invalid_run_id} = SquidMesh.inspect_run("not-a-uuid", repo: Repo)
    end

    test "returns stable workflow and step identifiers from persisted runs" do
      assert {:ok, created_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 repo: Repo
               )

      persisted_run = Repo.get!(SquidMesh.Persistence.Run, created_run.id)

      assert persisted_run.workflow == "Elixir.SquidMeshTest.InvoiceReminderWorkflow"
      assert persisted_run.current_step == "load_invoice"

      assert {:ok, inspected_run} = SquidMesh.inspect_run(created_run.id, repo: Repo)

      assert inspected_run.workflow == InvoiceReminderWorkflow
      assert inspected_run.trigger == :invoice_delivery
      assert inspected_run.current_step == :load_invoice
    end

    test "optionally includes step and attempt history" do
      assert {:ok, created_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, inspected_run} =
               SquidMesh.inspect_run(created_run.id, include_history: true, repo: Repo)

      assert Enum.map(inspected_run.steps, &{&1.step, &1.status, &1.depends_on}) == [
               {:load_invoice, :completed, []},
               {:send_email, :completed, []}
             ]

      assert [%SquidMesh.Steps.Execution{}, %SquidMesh.Steps.Execution{}] =
               inspected_run.step_runs

      assert Enum.map(inspected_run.step_runs, &{&1.step, &1.status}) == [
               {:load_invoice, :completed},
               {:send_email, :completed}
             ]

      assert Enum.all?(inspected_run.step_runs, fn step_run ->
               match?([%SquidMesh.Steps.Attempt{}], step_run.attempts)
             end)

      assert Enum.map(inspected_run.step_runs, fn step_run ->
               {step_run.step, Enum.map(step_run.attempts, & &1.attempt_number)}
             end) == [
               {:load_invoice, [1]},
               {:send_email, [1]}
             ]

      assert inspected_run.audit_events == []
    end

    test "surfaces paused and resumed audit events for manual pause workflows" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      assert {:ok, paused_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert paused_run.status == :paused

      assert Enum.map(paused_run.audit_events, &{&1.type, &1.step, &1.actor}) == [
               {:paused, :wait_for_approval, nil}
             ]

      assert {:ok, resumed_run} =
               SquidMesh.unblock_run(
                 run.id,
                 %{
                   actor: "ops_123",
                   comment: "resume requested",
                   metadata: %{ticket: "ops-123"}
                 },
                 repo: Repo
               )

      assert resumed_run.status == :running

      assert %{success: success, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert success >= 1

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert Enum.map(completed_run.audit_events, &{&1.type, &1.step, &1.actor, &1.comment}) == [
               {:paused, :wait_for_approval, nil, nil},
               {:resumed, :wait_for_approval, "ops_123", "resume requested"}
             ]

      assert Enum.map(completed_run.audit_events, & &1.metadata) == [
               nil,
               %{ticket: "ops-123"}
             ]
    end

    test "surfaces irreversible recovery policy in step history" do
      assert {:ok, created_run} =
               SquidMesh.start_run(
                 IrreversibleWorkflow,
                 %{account_id: "acct_123"},
                 repo: Repo
               )

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, inspected_run} =
               SquidMesh.inspect_run(created_run.id, include_history: true, repo: Repo)

      assert %StepState{recovery: %{replay: :manual_review_required}} =
               Enum.find(inspected_run.steps, &(&1.step == :capture_payment))

      assert %SquidMesh.Steps.Execution{recovery: %{irreversible?: true, compensatable?: false}} =
               Enum.find(inspected_run.step_runs, &(&1.step == :capture_payment))
    end

    test "surfaces paused audit events even when the workflow definition can no longer load" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      Repo.update_all(
        from(run_record in SquidMesh.Persistence.Run, where: run_record.id == ^run.id),
        set: [workflow: "Elixir.Missing.Workflow"]
      )

      assert {:ok, paused_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert paused_run.workflow == "Elixir.Missing.Workflow"
      assert paused_run.status == :paused

      assert Enum.map(paused_run.audit_events, &{&1.type, &1.step}) == [
               {:paused, "wait_for_approval"}
             ]
    end

    test "reconstructs completed pause audit events from persisted resume metadata when the workflow definition can no longer load" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      assert {:ok, _resumed_run} =
               SquidMesh.unblock_run(run.id, %{actor: "ops_123", comment: "resume requested"},
                 repo: Repo
               )

      assert %{success: success, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert success >= 1

      assert {1, _rows} =
               Repo.update_all(
                 from(step_run in SquidMesh.Persistence.StepRun,
                   where:
                     step_run.run_id == ^run.id and step_run.step == "wait_for_approval" and
                       step_run.status == "completed"
                 ),
                 set: [manual: nil]
               )

      Repo.update_all(
        from(run_record in SquidMesh.Persistence.Run, where: run_record.id == ^run.id),
        set: [workflow: "Elixir.Missing.Workflow"]
      )

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.workflow == "Elixir.Missing.Workflow"

      assert Enum.map(completed_run.audit_events, &{&1.type, &1.step, &1.actor, &1.comment}) == [
               {:paused, "wait_for_approval", nil, nil},
               {:resumed, "wait_for_approval", nil, nil}
             ]
    end

    test "reconstructs legacy approval audit events when the workflow definition can no longer load" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {:ok, _approved_run} =
               SquidMesh.approve_run(run.id, %{actor: "ops_123", comment: "approved"}, repo: Repo)

      assert %{success: success, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert success >= 1

      assert {1, _rows} =
               Repo.update_all(
                 from(step_run in SquidMesh.Persistence.StepRun,
                   where:
                     step_run.run_id == ^run.id and step_run.step == "wait_for_review" and
                       step_run.status == "completed"
                 ),
                 set: [manual: nil]
               )

      Repo.update_all(
        from(run_record in SquidMesh.Persistence.Run, where: run_record.id == ^run.id),
        set: [workflow: "Elixir.Missing.Workflow"]
      )

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert completed_run.workflow == "Elixir.Missing.Workflow"

      assert Enum.map(completed_run.audit_events, &{&1.type, &1.step, &1.actor, &1.comment}) == [
               {:paused, "wait_for_review", nil, nil},
               {:approved, "wait_for_review", "ops_123", "approved"}
             ]
    end

    test "falls back to legacy approval output when persisted manual audit metadata is corrupted" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {:ok, _approved_run} =
               SquidMesh.approve_run(run.id, %{actor: "ops_123", comment: "approved"}, repo: Repo)

      assert %{success: success, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert success >= 1

      assert {1, _rows} =
               Repo.update_all(
                 from(step_run in SquidMesh.Persistence.StepRun,
                   where:
                     step_run.run_id == ^run.id and step_run.step == "wait_for_review" and
                       step_run.status == "completed"
                 ),
                 set: [manual: %{"event" => "unknown", "actor" => "ignored"}]
               )

      assert {:ok, completed_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      assert Enum.map(completed_run.audit_events, &{&1.type, &1.step, &1.actor, &1.comment}) == [
               {:paused, :wait_for_review, nil, nil},
               {:approved, :wait_for_review, "ops_123", "approved"}
             ]
    end
  end

  describe "inspect_run_graph/2" do
    setup :use_runtime_tables

    test "returns graph-oriented state for a completed transition workflow" do
      assert {:ok, created_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph} =
               SquidMesh.inspect_run_graph(created_run.id, repo: Repo)

      assert graph.run_id == created_run.id
      assert graph.workflow == InvoiceReminderWorkflow
      assert graph.source == :runtime_tables
      assert graph.status == :completed
      assert graph.current_node_id == nil
      assert graph.current_node_ids == []

      nodes = Map.new(graph.nodes, &{&1.id, &1})
      edges = Map.new(graph.edges, &{&1.id, &1})

      assert nodes["load_invoice"].status == :completed
      assert nodes["send_email"].status == :completed

      assert edges["load_invoice:ok:send_email"].type == :transition
      assert edges["load_invoice:ok:send_email"].status == :selected
      assert edges["send_email:ok:complete"].to == "complete"
      assert edges["send_email:ok:complete"].status == :selected

      refute nodes["load_invoice"].output
      assert nodes["load_invoice"].attempts == []

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph_with_history} =
               SquidMesh.inspect_run_graph(created_run.id, include_history: true, repo: Repo)

      nodes_with_history = Map.new(graph_with_history.nodes, &{&1.id, &1})

      assert nodes_with_history["load_invoice"].output.invoice.status == "open"

      assert [%{attempt_number: 1, status: :completed}] =
               nodes_with_history["load_invoice"].attempts
    end

    test "returns dependency edges from runtime-table state" do
      assert {:ok, created_run} =
               SquidMesh.start_run(
                 JournalDependencyWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph} =
               SquidMesh.inspect_run_graph(created_run.id, repo: Repo)

      nodes = Map.new(graph.nodes, &{&1.id, &1})
      edges = Map.new(graph.edges, &{&1.id, &1})

      assert nodes["load_account"].status in [:pending, :running]
      assert nodes["load_invoice"].status in [:pending, :running]
      assert nodes["send_email"].status == :waiting
      assert MapSet.new(graph.current_node_ids) == MapSet.new(["load_account", "load_invoice"])
      assert nodes["load_account"].current?
      assert nodes["load_invoice"].current?
      refute nodes["send_email"].current?
      assert edges["load_account:dependency:send_email"].status == :pending
      assert edges["load_invoice:dependency:send_email"].status == :pending

      SquidMesh.Test.Executor.drain()
    end

    test "surfaces paused manual state only when history is requested" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph} =
               SquidMesh.inspect_run_graph(run.id, repo: Repo)

      nodes = Map.new(graph.nodes, &{&1.id, &1})

      assert graph.current_node_id == "wait_for_review"
      assert nodes["wait_for_review"].status == :paused
      refute nodes["wait_for_review"].manual_state

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph_with_history} =
               SquidMesh.inspect_run_graph(run.id, include_history: true, repo: Repo)

      nodes_with_history = Map.new(graph_with_history.nodes, &{&1.id, &1})

      assert nodes_with_history["wait_for_review"].manual_state == %{
               status: :paused,
               step: "wait_for_review"
             }
    end

    test "keeps runtime-table retrying nodes non-terminal" do
      assert {:ok, run} =
               SquidMesh.start_run(JournalRetryWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "retry_gateway"}
               })

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph} =
               SquidMesh.inspect_run_graph(run.id, repo: Repo)

      nodes = Map.new(graph.nodes, &{&1.id, &1})
      edges = Map.new(graph.edges, &{&1.id, &1})

      assert graph.status == :retrying
      assert graph.current_node_id == "retry_gateway"
      assert nodes["retry_gateway"].status == :retrying
      assert nodes["retry_gateway"].current?
      assert edges["retry_gateway:ok:complete"].status == :pending

      SquidMesh.Test.Executor.drain()
    end
  end

  describe "list_runs/2" do
    setup :use_runtime_tables

    test "returns runs newest first" do
      assert {:ok, first_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      Process.sleep(1)

      assert {:ok, second_run} =
               SquidMesh.start_run(PaymentRecoveryWorkflow, %{account_id: "acct_456"}, repo: Repo)

      assert {:ok, runs} = SquidMesh.list_runs([], repo: Repo)

      assert Enum.map(runs, & &1.id) == [second_run.id, first_run.id]
    end

    test "filters runs by workflow" do
      assert {:ok, _first_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      assert {:ok, second_run} =
               SquidMesh.start_run(PaymentRecoveryWorkflow, %{account_id: "acct_456"}, repo: Repo)

      assert {:ok, runs} =
               SquidMesh.list_runs([workflow: PaymentRecoveryWorkflow], repo: Repo)

      assert Enum.map(runs, & &1.id) == [second_run.id]
      assert Enum.map(runs, & &1.workflow) == [PaymentRecoveryWorkflow]
    end

    test "filters runs by status" do
      assert {:ok, pending_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      assert {:ok, _failed_run} = Runs.Store.transition_run(Repo, pending_run.id, :failed)

      assert {:ok, runs} = SquidMesh.list_runs([status: :failed], repo: Repo)

      assert Enum.map(runs, & &1.id) == [pending_run.id]
      assert Enum.map(runs, & &1.status) == [:failed]
    end

    test "limits the number of returned runs" do
      assert {:ok, _first_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      Process.sleep(1)

      assert {:ok, second_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_456", invoice_id: "inv_456"},
                 repo: Repo
               )

      assert {:ok, runs} = SquidMesh.list_runs([limit: 1], repo: Repo)

      assert Enum.map(runs, & &1.id) == [second_run.id]
    end
  end

  describe "cancel_run/2" do
    setup :use_runtime_tables

    test "cancels pending runs through the public API" do
      assert {:ok, run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      assert {:ok, cancelled_run} = SquidMesh.cancel_run(run.id, repo: Repo)

      assert cancelled_run.id == run.id
      assert cancelled_run.status == :cancelled
    end

    test "marks active runs as cancelling through the public API" do
      assert {:ok, run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      assert {:ok, running_run} = Runs.Store.transition_run(Repo, run.id, :running)
      assert {:ok, cancelling_run} = SquidMesh.cancel_run(running_run.id, repo: Repo)

      assert cancelling_run.status == :cancelling
    end

    test "returns not found for missing runs" do
      assert {:error, :not_found} = SquidMesh.cancel_run(Ecto.UUID.generate(), repo: Repo)
    end

    test "returns a structured error for malformed run ids" do
      assert {:error, :invalid_run_id} = SquidMesh.cancel_run("not-a-uuid", repo: Repo)
    end

    test "finalizes paused step history when cancelling a paused run" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      assert {:ok, cancelled_run} = SquidMesh.cancel_run(run.id, repo: Repo)
      assert cancelled_run.status == :cancelled

      assert {:ok, inspected_run} =
               SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)

      paused_step = Enum.find(inspected_run.step_runs, &(&1.step == :wait_for_approval))

      assert paused_step.status == :failed
      assert paused_step.output == nil

      assert paused_step.last_error == %{
               message: "run cancelled while paused",
               reason: "cancelled"
             }

      assert Enum.map(paused_step.attempts, &{&1.attempt_number, &1.status, &1.error}) == [
               {1, :failed, %{message: "run cancelled while paused", reason: "cancelled"}}
             ]
    end
  end

  describe "replay_run/2" do
    setup :use_runtime_tables

    test "creates a new run linked to the source run through the public API" do
      assert {:ok, source_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      assert {:ok, replay_run} = SquidMesh.replay_run(source_run.id, repo: Repo)

      assert replay_run.id != source_run.id
      assert replay_run.workflow == InvoiceReminderWorkflow
      assert replay_run.trigger == :invoice_delivery
      assert replay_run.status == :pending
      assert replay_run.payload == source_run.payload
      assert replay_run.current_step == :load_invoice
      assert replay_run.replayed_from_run_id == source_run.id
    end

    test "returns not found when replaying a missing run" do
      assert {:error, :not_found} = SquidMesh.replay_run(Ecto.UUID.generate(), repo: Repo)
    end

    test "returns a structured error for malformed run ids" do
      assert {:error, :invalid_run_id} = SquidMesh.replay_run("not-a-uuid", repo: Repo)
    end

    test "rolls back replay creation when dispatching the replayed run fails" do
      assert {:ok, source_run} =
               Runs.Store.create_run(
                 Repo,
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"}
               )

      before_count = Repo.aggregate(SquidMesh.Persistence.Run, :count, :id)

      assert {:error, {:dispatch_failed, :executor_unavailable}} =
               SquidMesh.replay_run(
                 source_run.id,
                 repo: Repo,
                 executor: MissingExecutor
               )

      assert Repo.aggregate(SquidMesh.Persistence.Run, :count, :id) == before_count
    end

    test "blocks replay by default after completed irreversible steps" do
      assert {:ok, source_run} =
               SquidMesh.start_run(
                 IrreversibleWorkflow,
                 %{account_id: "acct_123"},
                 repo: Repo
               )

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      before_count = Repo.aggregate(SquidMesh.Persistence.Run, :count, :id)

      assert {:error,
              {:unsafe_replay,
               %{
                 message:
                   "replay requires explicit approval after irreversible or non-compensatable steps",
                 steps: [
                   %{
                     step: :capture_payment,
                     irreversible?: true,
                     compensatable?: false,
                     replay: :manual_review_required,
                     recovery: :manual_intervention
                   }
                 ]
               }}} = SquidMesh.replay_run(source_run.id, repo: Repo)

      assert Repo.aggregate(SquidMesh.Persistence.Run, :count, :id) == before_count
    end

    test "uses persisted recovery policy when checking replay safety" do
      assert {:ok, source_run} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_123"},
                 repo: Repo
               )

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {1, _rows} =
               Repo.update_all(
                 from(step_run in SquidMesh.Persistence.StepRun,
                   where:
                     step_run.run_id == ^source_run.id and step_run.step == "send_email" and
                       step_run.status == "completed"
                 ),
                 set: [
                   recovery: %{
                     "irreversible?" => false,
                     "compensatable?" => false,
                     "replay" => "manual_review_required",
                     "recovery" => "manual_intervention"
                   }
                 ]
               )

      assert {:error, {:unsafe_replay, %{steps: [%{step: :send_email}]}}} =
               SquidMesh.replay_run(source_run.id, repo: Repo)
    end

    test "allows replay after irreversible steps only when explicitly requested" do
      assert {:ok, source_run} =
               SquidMesh.start_run(
                 IrreversibleWorkflow,
                 %{account_id: "acct_123"},
                 repo: Repo
               )

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      assert {:ok, replay_run} =
               SquidMesh.replay_run(source_run.id, repo: Repo, allow_irreversible: true)

      assert replay_run.replayed_from_run_id == source_run.id
      assert replay_run.current_step == :load_account
    end

    test "does not treat non-boolean allow_irreversible values as approval" do
      assert {:ok, source_run} =
               SquidMesh.start_run(
                 IrreversibleWorkflow,
                 %{account_id: "acct_123"},
                 repo: Repo
               )

      assert %{success: 2, failure: 0} =
               SquidMesh.Test.Executor.drain()

      before_count = Repo.aggregate(SquidMesh.Persistence.Run, :count, :id)

      assert {:error, {:unsafe_replay, %{steps: [%{step: :capture_payment}]}}} =
               SquidMesh.replay_run(source_run.id, repo: Repo, allow_irreversible: "true")

      assert Repo.aggregate(SquidMesh.Persistence.Run, :count, :id) == before_count
    end
  end

  describe "unblock_run/2" do
    setup :use_runtime_tables

    test "resumes paused runs through the public API" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      assert {:ok, paused_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert paused_run.status == :paused

      assert {:ok, unblocked_run} = SquidMesh.unblock_run(run.id, repo: Repo)

      assert unblocked_run.id == run.id
      assert unblocked_run.status == :running
      assert unblocked_run.current_step == :record_delivery
    end

    test "rolls back unblock when dispatching the resumed step fails" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      assert {:ok, paused_run} = SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)
      assert paused_run.status == :paused

      assert {:error, {:dispatch_failed, :executor_unavailable}} =
               SquidMesh.unblock_run(run.id, repo: Repo, executor: MissingExecutor)

      assert {:ok, current_run} = SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)
      assert current_run.status == :paused
      assert current_run.current_step == :wait_for_approval

      paused_step = Enum.find(current_run.step_runs, &(&1.step == :wait_for_approval))
      assert paused_step.status == :running
      assert Enum.map(paused_step.attempts, & &1.status) == [:running]
    end

    test "returns not found for missing runs" do
      assert {:error, :not_found} = SquidMesh.unblock_run(Ecto.UUID.generate(), repo: Repo)
    end

    test "returns a structured error for malformed run ids" do
      assert {:error, :invalid_run_id} = SquidMesh.unblock_run("not-a-uuid", repo: Repo)
    end

    test "returns a structured error when the paused run workflow can no longer be loaded" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      Repo.update_all(
        from(run_record in SquidMesh.Persistence.Run, where: run_record.id == ^run.id),
        set: [workflow: "Elixir.Missing.Workflow"]
      )

      assert {:error, {:invalid_workflow, "Elixir.Missing.Workflow"}} =
               SquidMesh.unblock_run(run.id, repo: Repo)
    end

    test "returns a structured error when the paused step no longer resolves to built-in :pause" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      Repo.update_all(
        from(run_record in SquidMesh.Persistence.Run, where: run_record.id == ^run.id),
        set: [current_step: "record_delivery"]
      )

      assert {:error, {:invalid_step, :record_delivery}} =
               SquidMesh.unblock_run(run.id, repo: Repo)
    end

    test "does not mutate pause state when a stale unblock races with cancellation" do
      assert {:ok, run} =
               SquidMesh.start_run(PauseWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_approval"}
               })

      assert {:ok, paused_run} = SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)
      assert paused_run.status == :paused

      assert {:ok, cancelled_run} = SquidMesh.cancel_run(run.id, repo: Repo)
      assert cancelled_run.status == :cancelled

      assert {:error, {:invalid_transition, :cancelled, :running}} =
               Unblocker.unblock(SquidMesh.config!(repo: Repo), paused_run)

      assert {:ok, current_run} = SquidMesh.inspect_run(run.id, include_history: true, repo: Repo)
      assert current_run.status == :cancelled

      paused_step = Enum.find(current_run.step_runs, &(&1.step == :wait_for_approval))
      assert paused_step.status == :failed
      assert paused_step.output == nil

      assert Enum.map(paused_step.attempts, &{&1.status, &1.error}) == [
               {:failed, %{message: "run cancelled while paused", reason: "cancelled"}}
             ]
    end
  end

  describe "approve_run/3 and reject_run/3" do
    setup :use_runtime_tables

    test "approves paused approval runs through the public API" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {:ok, paused_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert paused_run.status == :paused

      assert {:ok, approved_run} =
               SquidMesh.approve_run(
                 run.id,
                 %{actor: "ops_123", comment: "approved"},
                 repo: Repo
               )

      assert approved_run.id == run.id
      assert approved_run.status == :running
      assert approved_run.current_step == :record_approval
    end

    test "rejects paused approval runs through the public API" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {:ok, paused_run} = SquidMesh.inspect_run(run.id, repo: Repo)
      assert paused_run.status == :paused

      assert {:ok, rejected_run} =
               SquidMesh.reject_run(
                 run.id,
                 %{actor: "ops_456", comment: "rejected"},
                 repo: Repo
               )

      assert rejected_run.id == run.id
      assert rejected_run.status == :running
      assert rejected_run.current_step == :record_rejection
    end

    test "returns a structured error when the approval workflow can no longer be loaded" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      Repo.update_all(
        from(run_record in SquidMesh.Persistence.Run, where: run_record.id == ^run.id),
        set: [workflow: "Elixir.Missing.Workflow"]
      )

      assert {:error, {:invalid_workflow, "Elixir.Missing.Workflow"}} =
               SquidMesh.approve_run(run.id, %{actor: "ops_123"}, repo: Repo)
    end

    test "returns a structured error for malformed approval run ids" do
      assert {:error, :invalid_run_id} =
               SquidMesh.approve_run("not-a-uuid", %{actor: "ops_123"}, repo: Repo)
    end

    test "returns a structured error for malformed rejection run ids" do
      assert {:error, :invalid_run_id} =
               SquidMesh.reject_run("not-a-uuid", %{actor: "ops_123"}, repo: Repo)
    end

    test "rejects empty actor maps for approval decisions" do
      assert {:ok, run} =
               SquidMesh.start_run(ApprovalWorkflow, %{account_id: "acct_123"}, repo: Repo)

      assert :ok =
               StepWorker.perform(%Job{
                 args: %{"run_id" => run.id, "step" => "wait_for_review"}
               })

      assert {:error, {:invalid_review, %{actor: :required}}} =
               SquidMesh.approve_run(run.id, %{actor: %{}}, repo: Repo)
    end
  end

  describe "read model" do
    @read_model_storage {Jido.Storage.ETS, table: :squid_mesh_read_model_squid_mesh_test}
    @read_model_run_id "run_123"
    @read_model_workflow "BillingWorkflow"
    @read_model_queue "default"
    @read_model_runnable_key "run_123:charge_card:1"
    @read_model_idempotency_key "run_123:charge_card:payment_456"
    @read_model_started_at ~U[2026-05-15 00:00:00Z]
    @read_model_visible_at ~U[2026-05-15 00:00:10Z]

    setup do
      cleanup_read_model_storage()
      on_exit(&cleanup_read_model_storage/0)
    end

    test "inspect_run/2 can read from the read model" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      append_read_model_dispatch_entries([read_model_attempt_scheduled()])

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert snapshot.run_id == @read_model_run_id
      assert snapshot.workflow == @read_model_workflow
      assert snapshot.queue == @read_model_queue
      assert snapshot.reason == :attempt_visible

      assert [%{runnable_key: @read_model_runnable_key, status: :available}] =
               snapshot.visible_attempts
    end

    test "start_run/3 can use the journal runtime without writing legacy runtime tables" do
      legacy_counts_before = legacy_runtime_counts()

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert snapshot.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert snapshot.queue == @read_model_queue
      assert snapshot.reason == :attempt_visible

      assert [%{runnable_key: runnable_key, step: "check_gateway", status: :available}] =
               snapshot.visible_attempts

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph} =
               SquidMesh.inspect_run_graph(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      nodes = Map.new(graph.nodes, &{&1.id, &1})
      edges = Map.new(graph.edges, &{&1.id, &1})

      assert graph.source == :read_model
      assert graph.current_node_id == "check_gateway"
      assert graph.current_node_ids == ["check_gateway"]
      assert nodes["check_gateway"].status == :pending
      assert nodes["check_gateway"].attempts == []
      assert edges["check_gateway:ok:complete"].status == :pending

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [:run_started, :runnables_planned]

      assert [%{runnable_key: ^runnable_key, step: "check_gateway"}] =
               run_entries
               |> List.last()
               |> Map.fetch!(:data)
               |> Map.fetch!(:runnables)

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [:run_queued, :attempt_scheduled]
      assert [%{runnable_key: ^runnable_key, step: "check_gateway"}] = snapshot.visible_attempts

      assert {:ok, run_index_projection} =
               Journal.rebuild_run_index_projection(
                 @read_model_storage,
                 Atom.to_string(PaymentRecoveryWorkflow)
               )

      assert SquidMesh.Runtime.RunIndexProjection.run_ids(run_index_projection) == [
               snapshot.run_id
             ]

      assert legacy_runtime_counts() == legacy_counts_before
    end

    test "list_runs/2 lists journal runs for one workflow newest first" do
      legacy_counts_before = legacy_runtime_counts()

      assert {:ok, %Snapshot{} = older_run} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_older"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = newer_run} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_newer"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 1, :second)
               )

      assert {:ok, [%Summary{} = first, %Summary{} = second]} =
               SquidMesh.list_runs([workflow: PaymentRecoveryWorkflow],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert [first.run_id, second.run_id] == [newer_run.run_id, older_run.run_id]
      assert Enum.all?([first, second], &(&1.workflow == Atom.to_string(PaymentRecoveryWorkflow)))
      assert Enum.all?([first, second], &(&1.queue == @read_model_queue))
      assert Enum.all?([first, second], &(&1.status == :running))
      assert legacy_runtime_counts() == legacy_counts_before
    end

    test "list_runs/2 lists journal runs across workflows from the global catalog" do
      assert {:ok, %Snapshot{} = payment_run} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_payment"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = approval_run} =
               SquidMesh.start_run(
                 ApprovalWorkflow,
                 %{account_id: "acct_approval"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 1, :second)
               )

      assert {:ok, [%Summary{} = first, %Summary{} = second]} =
               SquidMesh.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "caller-default-queue",
                 now: @read_model_visible_at
               )

      assert [first.run_id, second.run_id] == [approval_run.run_id, payment_run.run_id]

      assert [first.workflow, second.workflow] == [
               Atom.to_string(ApprovalWorkflow),
               Atom.to_string(PaymentRecoveryWorkflow)
             ]

      assert {:ok, [%Summary{} = filtered]} =
               SquidMesh.list_runs([workflow: PaymentRecoveryWorkflow],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert filtered.run_id == payment_run.run_id
      assert filtered.workflow == Atom.to_string(PaymentRecoveryWorkflow)
    end

    test "list_runs/2 applies journal status and limit filters after rebuilding snapshots" do
      assert {:ok, %Snapshot{} = completed_run} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_completed"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{status: :completed}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "list_runs_worker",
                 claim_id: "list_runs_claim",
                 claim_token: "list_runs_token",
                 now: @read_model_visible_at
               )

      assert {:ok, _running_run} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_running"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 1, :second)
               )

      assert {:ok, [%Summary{} = listed_run]} =
               SquidMesh.list_runs(
                 [workflow: PaymentRecoveryWorkflow, status: :completed, limit: 1],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert listed_run.run_id == completed_run.run_id
      assert listed_run.status == :completed
    end

    test "list_runs/2 uses the queue recorded in each run catalog fact" do
      first_queue = "journal-list-first-queue"
      second_queue = "journal-list-second-queue"

      assert {:ok, %Snapshot{} = first_run} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_first_queue"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: first_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = second_run} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_second_queue"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: second_queue,
                 now: DateTime.add(@read_model_started_at, 1, :second)
               )

      assert {:ok, listed_runs} =
               SquidMesh.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "caller-default-queue"
               )

      listed_payment_runs =
        Enum.filter(listed_runs, &(&1.workflow == Atom.to_string(PaymentRecoveryWorkflow)))

      assert [%Summary{} = listed_second, %Summary{} = listed_first | _older_runs] =
               listed_payment_runs

      assert {listed_second.run_id, listed_second.queue} == {second_run.run_id, second_queue}
      assert {listed_first.run_id, listed_first.queue} == {first_run.run_id, first_queue}

      assert {:ok, %Snapshot{} = inspected} =
               SquidMesh.inspect_run(listed_second.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: listed_second.queue,
                 now: @read_model_visible_at
               )

      assert inspected.run_id == listed_second.run_id
      assert inspected.queue == listed_second.queue
      assert [%{step: "check_gateway", status: :available}] = inspected.visible_attempts

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph} =
               SquidMesh.inspect_run_graph(listed_second.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: listed_second.queue,
                 now: @read_model_visible_at
               )

      assert graph.run_id == listed_second.run_id
      assert Enum.map(graph.nodes, &{&1.id, &1.status}) == [{"check_gateway", :pending}]
    end

    test "list_runs/2 surfaces malformed journal run catalog facts" do
      workflow = Atom.to_string(PaymentRecoveryWorkflow)

      malformed_entry = %SquidMesh.Runtime.DispatchProtocol.Entry{
        type: :run_cataloged,
        thread: {:run_catalog, "all"},
        data: %{
          run_id: Ecto.UUID.generate(),
          workflow: workflow
        },
        occurred_at: @read_model_started_at
      }

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [malformed_entry])

      assert {:error, {:run_catalog_anomalies, anomalies}} =
               SquidMesh.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage
               )

      assert [%{entry_type: :run_cataloged, reason: :malformed_entry, workflow: ^workflow}] =
               anomalies
    end

    test "list_runs/2 surfaces conflicting journal run catalog facts" do
      run_id = Ecto.UUID.generate()
      workflow = Atom.to_string(PaymentRecoveryWorkflow)

      first_entry = %SquidMesh.Runtime.DispatchProtocol.Entry{
        type: :run_cataloged,
        thread: {:run_catalog, "all"},
        data: %{
          run_id: run_id,
          workflow: workflow,
          queue: "first-queue"
        },
        occurred_at: @read_model_started_at
      }

      second_entry = %SquidMesh.Runtime.DispatchProtocol.Entry{
        type: :run_cataloged,
        thread: {:run_catalog, "all"},
        data: %{
          run_id: run_id,
          workflow: workflow,
          queue: "second-queue"
        },
        occurred_at: @read_model_started_at
      }

      assert {:ok, _thread} =
               Journal.append_entries(@read_model_storage, [first_entry, second_entry])

      assert {:error, {:run_catalog_anomalies, anomalies}} =
               SquidMesh.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage
               )

      assert [
               %{
                 entry_type: :run_cataloged,
                 reason: :conflicting_run_catalog,
                 run_id: ^run_id,
                 workflow: ^workflow,
                 queue: "second-queue"
               }
             ] = anomalies
    end

    test "list_runs/2 rejects catalog facts that disagree with the run thread" do
      run_id = Ecto.UUID.generate()
      actual_workflow = Atom.to_string(PaymentRecoveryWorkflow)
      catalog_workflow = Atom.to_string(ApprovalWorkflow)

      assert {:ok, run_started} =
               DispatchProtocol.new_entry(:run_started, %{
                 run_id: run_id,
                 workflow: actual_workflow,
                 occurred_at: @read_model_started_at
               })

      assert {:ok, runnables_planned} =
               DispatchProtocol.new_entry(:runnables_planned, %{
                 run_id: run_id,
                 runnables: [journal_start_runnable(run_id)],
                 occurred_at: @read_model_started_at
               })

      assert {:ok, catalog_entry} =
               DispatchProtocol.new_entry(:run_cataloged, %{
                 run_id: run_id,
                 workflow: catalog_workflow,
                 queue: @read_model_queue,
                 occurred_at: @read_model_started_at
               })

      assert {:ok, _thread} =
               Journal.append_entries(@read_model_storage, [run_started, runnables_planned])

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [catalog_entry])

      assert {:error,
              {:run_catalog_summary_failed, ^run_id,
               {:catalog_workflow_mismatch,
                %{expected: ^catalog_workflow, actual: ^actual_workflow, run_id: ^run_id}}}} =
               SquidMesh.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage
               )
    end

    test "start_run/3 rejects conflicting catalog facts before dispatch visibility" do
      run_id = Ecto.UUID.generate()

      assert {:ok, bad_catalog_entry} =
               DispatchProtocol.new_entry(:run_cataloged, %{
                 run_id: run_id,
                 workflow: Atom.to_string(PaymentRecoveryWorkflow),
                 queue: "wrong-queue",
                 occurred_at: @read_model_started_at
               })

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [bad_catalog_entry])

      assert {:error, {:journal_start_committed, ^run_id, {:conflicting_run_catalog, ^run_id}}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:error, :not_found} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})
    end

    test "start_run/3 rejects conflicting run index facts before dispatch visibility" do
      run_id = Ecto.UUID.generate()

      assert {:ok, bad_index_entry} =
               DispatchProtocol.new_entry(:run_indexed, %{
                 run_id: run_id,
                 workflow: Atom.to_string(PaymentRecoveryWorkflow),
                 queue: "wrong-queue",
                 occurred_at: @read_model_started_at
               })

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [bad_index_entry])

      assert {:error, {:journal_start_committed, ^run_id, {:conflicting_run_index, ^run_id}}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:error, :not_found} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})
    end

    test "configured journal runtime defaults start, inspect, explain, and execute calls" do
      configured_queue = "configured-runtime-test"

      put_squid_mesh_config(
        runtime: :journal,
        read_model: :read_model,
        journal_storage: @read_model_storage,
        queue: configured_queue
      )

      legacy_counts_before = legacy_runtime_counts()

      assert {:ok, %Snapshot{} = started} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 now: @read_model_started_at
               )

      assert started.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert started.queue == configured_queue
      assert [%{step: "check_gateway", status: :available}] = started.visible_attempts

      assert {:ok, %Snapshot{} = inspected} =
               SquidMesh.inspect_run(started.run_id, now: @read_model_started_at)

      assert inspected.run_id == started.run_id
      assert inspected.queue == configured_queue

      assert {:ok, %Diagnostic{} = explanation} =
               SquidMesh.explain_run(started.run_id, now: @read_model_started_at)

      assert explanation.run_id == started.run_id
      assert explanation.queue == configured_queue
      assert explanation.reason == :attempt_visible

      assert {:error, {:invalid_option, {:queue, :invalid}}} =
               SquidMesh.inspect_run(started.run_id, queue: "../dispatch")

      assert {:error, {:invalid_option, {:queue, :invalid}}} =
               SquidMesh.execute_next(queue: "../dispatch")

      assert {:ok, %Snapshot{} = completed} =
               SquidMesh.execute_next(
                 owner_id: "configured-runtime-test",
                 now: @read_model_started_at
               )

      assert completed.run_id == started.run_id
      assert completed.terminal?
      assert completed.terminal_status == :completed

      assert legacy_runtime_counts() == legacy_counts_before
    end

    test "journal runtime executes built-in log steps" do
      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 ManualAndScheduledDigestWorkflow,
                 :manual_digest,
                 %{chat_id: 123},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert snapshot.run_id == run_id
      assert [%{step: "announce_prompt", status: :available}] = snapshot.visible_attempts

      log =
        capture_log([level: :warning], fn ->
          assert {:ok, %Snapshot{} = completed_snapshot} =
                   execute_journal_next(
                     runtime: :journal,
                     journal_storage: @read_model_storage,
                     queue: @read_model_queue,
                     owner_id: "journal-log-test",
                     now: @read_model_started_at,
                     finished_at: @read_model_visible_at
                   )

          send(self(), {:completed_snapshot, completed_snapshot})
        end)

      assert log =~ "posting digest"
      assert_receive {:completed_snapshot, %Snapshot{} = completed_snapshot}

      assert completed_snapshot.run_id == run_id
      assert completed_snapshot.terminal?
      assert completed_snapshot.terminal_status == :completed
      assert completed_snapshot.visible_attempts == []

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]
    end

    test "journal runtime executes built-in wait steps by delaying the successor" do
      run_id = Ecto.UUID.generate()
      delayed_at = DateTime.add(@read_model_visible_at, 2, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 WaitWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert snapshot.run_id == run_id
      assert [%{step: "wait_for_settlement", status: :available}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{} = delayed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-wait-test",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert delayed_snapshot.run_id == run_id
      assert delayed_snapshot.reason == :attempt_scheduled_for_later
      assert delayed_snapshot.visible_attempts == []
      assert delayed_snapshot.next_visible_at == delayed_at

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-wait-test",
                 now: @read_model_visible_at
               )

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert [%{data: delayed_attempt}] =
               Enum.filter(
                 dispatch_entries,
                 &(&1.type == :attempt_scheduled and &1.data.step == "record_settlement")
               )

      assert delayed_attempt.visible_at == delayed_at

      refute Enum.any?(
               dispatch_entries,
               &(&1.type == :attempt_claimed and
                   &1.data.runnable_key == delayed_attempt.runnable_key)
             )

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-wait-test",
                 now: delayed_at,
                 finished_at: delayed_at
               )

      assert completed_snapshot.run_id == run_id
      assert completed_snapshot.terminal?
      assert completed_snapshot.terminal_status == :completed

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]

      assert [_wait_runnable, delayed_runnable] =
               run_entries
               |> Enum.filter(&(&1.type == :runnables_planned))
               |> Enum.flat_map(&Map.fetch!(&1.data, :runnables))

      assert delayed_runnable.step == "record_settlement"
      assert delayed_runnable.visible_at == delayed_at
    end

    test "journal runtime executes built-in pause steps into durable manual state" do
      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert snapshot.run_id == run_id
      assert [%{step: "wait_for_approval", status: :available}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{} = paused_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-test",
                 claim_id: "claim_pause",
                 claim_token: "token_pause",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert paused_snapshot.run_id == run_id
      assert paused_snapshot.status == :paused
      assert paused_snapshot.reason == :manual_intervention_required
      assert paused_snapshot.visible_attempts == []
      assert paused_snapshot.pending_results == []

      assert paused_snapshot.manual_state == %{
               step: "wait_for_approval",
               kind: "pause",
               paused_at: @read_model_visible_at,
               metadata: %{
                 output: %{},
                 target: "record_delivery"
               }
             }

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph} =
               SquidMesh.inspect_run_graph(run_id,
                 read_model: :read_model,
                 include_history: true,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      graph_nodes = Map.new(graph.nodes, &{&1.id, &1})

      assert graph.current_node_id == "wait_for_approval"
      assert graph.current_node_ids == ["wait_for_approval"]
      assert graph_nodes["wait_for_approval"].status == :paused
      assert graph_nodes["wait_for_approval"].current?
      assert graph_nodes["wait_for_approval"].manual_state == paused_snapshot.manual_state

      assert {:error, :not_found} = SquidMesh.unblock_run(run_id, %{})

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-test",
                 now: @read_model_visible_at
               )

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :manual_step_paused
             ]

      assert %{
               data: %{
                 step: "wait_for_approval",
                 kind: "pause",
                 paused_at: @read_model_visible_at,
                 metadata: %{output: %{}, target: "record_delivery"}
               }
             } = List.last(run_entries)
    end

    test "journal runtime resumes built-in pause steps through durable manual resolution" do
      run_id = Ecto.UUID.generate()
      resumed_at = DateTime.add(@read_model_visible_at, 1, :second)
      resumed_at_iso = DateTime.to_iso8601(resumed_at)

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{step: "wait_for_approval", status: :available}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-resolution-test",
                 claim_id: "claim_pause_resolution",
                 claim_token: "token_pause_resolution",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = resumed_snapshot} =
               SquidMesh.unblock_run(
                 run_id,
                 %{actor: "ops_123", comment: "resume requested"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: resumed_at
               )

      assert resumed_snapshot.status == :running
      assert resumed_snapshot.reason == :attempt_visible
      assert resumed_snapshot.manual_state == nil
      assert resumed_snapshot.pending_dispatches == []

      assert [
               %{step: "record_delivery", status: :available, input: %{account_id: "acct_123"}}
             ] = resumed_snapshot.visible_attempts

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph} =
               SquidMesh.inspect_run_graph(run_id,
                 read_model: :read_model,
                 include_history: true,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: resumed_at
               )

      graph_nodes = Map.new(graph.nodes, &{&1.id, &1})

      assert graph.current_node_id == "record_delivery"
      assert graph.current_node_ids == ["record_delivery"]
      assert graph_nodes["wait_for_approval"].status == :completed
      assert graph_nodes["wait_for_approval"].manual_state == nil
      assert graph_nodes["record_delivery"].status == :pending
      assert graph_nodes["record_delivery"].current?

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :manual_step_paused,
               :manual_step_resolved,
               :runnables_planned
             ]

      assert %{
               type: :manual_step_resolved,
               data: %{
                 step: "wait_for_approval",
                 action: "resumed",
                 result: %{},
                 metadata: %{
                   "event" => "resumed",
                   "actor" => "ops_123",
                   "comment" => "resume requested",
                   "at" => ^resumed_at_iso
                 }
               }
             } = Enum.at(run_entries, 4)

      assert {:ok, %Snapshot{} = replayed_resume_snapshot} =
               SquidMesh.unblock_run(
                 run_id,
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: resumed_at
               )

      assert [
               %{step: "record_delivery", status: :available, input: %{account_id: "acct_123"}}
             ] = replayed_resume_snapshot.visible_attempts

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-resolution-test",
                 claim_id: "claim_record_delivery",
                 claim_token: "token_record_delivery",
                 now: resumed_at,
                 finished_at: resumed_at
               )

      assert completed_snapshot.status == :completed
      assert completed_snapshot.terminal?

      assert {:ok, replayed_run_entries} =
               Journal.load_entries(@read_model_storage, {:run, run_id})

      assert Enum.count(replayed_run_entries, &(&1.type == :manual_step_resolved)) == 1
    end

    test "journal runtime returns structured errors for invalid pause resume requests" do
      assert {:error, :not_found} =
               SquidMesh.unblock_run(Ecto.UUID.generate(),
                 journal_storage: @read_model_storage
               )

      assert {:error, :not_found} =
               SquidMesh.unblock_run(Ecto.UUID.generate(), %{},
                 journal_storage: @read_model_storage
               )

      assert {:error, :invalid_run_id} =
               SquidMesh.unblock_run(
                 "not-a-uuid",
                 %{},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, :not_found} =
               SquidMesh.unblock_run(
                 Ecto.UUID.generate(),
                 %{},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{}} =
               SquidMesh.start_run(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-invalid-pause-resolution-test",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert {:error, {:invalid_resume, %{actor: :invalid}}} =
               SquidMesh.unblock_run(
                 run_id,
                 %{actor: ""},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})
      refute Enum.any?(run_entries, &(&1.type == :manual_step_resolved))
    end

    test "journal runtime approves built-in approval steps through durable manual resolution" do
      run_id = Ecto.UUID.generate()
      approved_at = DateTime.add(@read_model_visible_at, 1, :second)
      approved_at_iso = DateTime.to_iso8601(approved_at)

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 ApprovalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{step: "wait_for_review", status: :available}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{} = paused_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-approval-test",
                 claim_id: "claim_approval",
                 claim_token: "token_approval",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert paused_snapshot.status == :paused
      assert paused_snapshot.reason == :manual_intervention_required

      assert paused_snapshot.manual_state == %{
               step: "wait_for_review",
               kind: "approval",
               paused_at: @read_model_visible_at,
               metadata: %{
                 ok_target: "record_approval",
                 error_target: "record_rejection",
                 output_key: "approval"
               }
             }

      assert {:ok, %Snapshot{} = approved_snapshot} =
               SquidMesh.approve_run(
                 run_id,
                 %{actor: "ops_123", comment: "approved"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: approved_at
               )

      assert approved_snapshot.status == :running
      assert approved_snapshot.reason == :attempt_visible
      assert approved_snapshot.manual_state == nil

      assert [
               %{
                 step: "record_approval",
                 status: :available,
                 input: %{
                   account_id: "acct_123",
                   approval: %{
                     decision: "approved",
                     actor: "ops_123",
                     comment: "approved",
                     decided_at: ^approved_at_iso
                   }
                 }
               }
             ] = approved_snapshot.visible_attempts

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :manual_step_paused,
               :manual_step_resolved,
               :runnables_planned
             ]

      assert %{
               type: :manual_step_resolved,
               data: %{
                 step: "wait_for_review",
                 action: "approved",
                 result: %{
                   approval: %{
                     decision: "approved",
                     actor: "ops_123",
                     comment: "approved",
                     decided_at: ^approved_at_iso
                   }
                 },
                 metadata: %{
                   "event" => "approved",
                   "actor" => "ops_123",
                   "comment" => "approved",
                   "at" => ^approved_at_iso
                 }
               }
             } = Enum.at(run_entries, 4)
    end

    test "journal runtime rejects built-in approval steps through durable manual resolution" do
      run_id = Ecto.UUID.generate()
      rejected_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 ApprovalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{step: "wait_for_review", status: :available}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-rejection-test",
                 claim_id: "claim_rejection",
                 claim_token: "token_rejection",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = rejected_snapshot} =
               SquidMesh.reject_run(
                 run_id,
                 %{actor: "ops_456", comment: "rejected"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: rejected_at
               )

      assert rejected_snapshot.status == :running

      assert [
               %{
                 step: "record_rejection",
                 status: :available,
                 input: %{approval: %{decision: "rejected", actor: "ops_456"}}
               }
             ] = rejected_snapshot.visible_attempts

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})
      assert Enum.count(run_entries, &(&1.type == :manual_step_resolved)) == 1
      assert Enum.at(run_entries, 4).data.action == "rejected"
    end

    test "journal runtime repairs pending dispatch after approval resolution was already committed" do
      run_id = Ecto.UUID.generate()
      approved_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 ApprovalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{step: "wait_for_review"}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-approval-resolution-crash-test",
                 claim_id: "claim_approval_resolution_crash",
                 claim_token: "token_approval_resolution_crash",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      result = %{
        approval: %{
          decision: "approved",
          actor: "ops_123",
          decided_at: DateTime.to_iso8601(approved_at)
        }
      }

      successor_runnable = %{
        run_id: run_id,
        runnable_key: "#{run_id}:record_approval:1",
        idempotency_key: "#{run_id}:record_approval:1",
        attempt_number: 1,
        queue: @read_model_queue,
        step: "record_approval",
        input: Map.merge(%{account_id: "acct_123"}, result),
        visible_at: approved_at
      }

      append_read_model_run_entries([
        read_model_entry!(:manual_step_resolved, %{
          run_id: run_id,
          step: "wait_for_review",
          action: "approved",
          result: result,
          metadata: %{"event" => "approved", "at" => DateTime.to_iso8601(approved_at)},
          occurred_at: approved_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [successor_runnable],
          occurred_at: approved_at
        })
      ])

      assert {:ok, %Snapshot{} = approved_snapshot} =
               SquidMesh.approve_run(
                 run_id,
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: approved_at
               )

      assert [
               %{
                 step: "record_approval",
                 status: :available,
                 input: %{approval: %{decision: "approved"}}
               }
             ] = approved_snapshot.visible_attempts

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})
      assert Enum.count(run_entries, &(&1.type == :manual_step_resolved)) == 1

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.count(dispatch_entries, &(&1.type == :attempt_scheduled)) == 2
    end

    test "journal runtime does not approve after terminal transition" do
      run_id = Ecto.UUID.generate()
      terminal_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{}} =
               SquidMesh.start_run(
                 ApprovalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-terminal-approval-resolution-test",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      append_read_model_run_entries([
        read_model_entry!(:run_terminal, %{
          run_id: run_id,
          status: :cancelled,
          occurred_at: terminal_at
        })
      ])

      assert {:error, {:invalid_transition, :cancelled, :running}} =
               SquidMesh.approve_run(
                 run_id,
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: terminal_at
               )

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})
      refute Enum.any?(run_entries, &(&1.type == :manual_step_resolved))
    end

    test "journal runtime returns structured errors for invalid approval controls" do
      assert {:error, {:invalid_review, %{actor: :required}}} =
               SquidMesh.approve_run(Ecto.UUID.generate(), %{},
                 journal_storage: @read_model_storage
               )

      assert {:error, :invalid_run_id} =
               SquidMesh.approve_run(
                 "not-a-uuid",
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, :not_found} =
               SquidMesh.reject_run(
                 Ecto.UUID.generate(),
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, {:invalid_review, %{actor: :required}}} =
               SquidMesh.approve_run(
                 Ecto.UUID.generate(),
                 %{actor: ""},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "journal runtime resumes pending dispatch after manual resolution was already committed" do
      run_id = Ecto.UUID.generate()
      resumed_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{step: "wait_for_approval"}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-resolution-crash-test",
                 claim_id: "claim_pause_resolution_crash",
                 claim_token: "token_pause_resolution_crash",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      successor_runnable = %{
        run_id: run_id,
        runnable_key: "#{run_id}:record_delivery:1",
        idempotency_key: "#{run_id}:record_delivery:1",
        attempt_number: 1,
        queue: @read_model_queue,
        step: "record_delivery",
        input: %{account_id: "acct_123"},
        visible_at: resumed_at
      }

      append_read_model_run_entries([
        read_model_entry!(:manual_step_resolved, %{
          run_id: run_id,
          step: "wait_for_approval",
          action: "resumed",
          result: %{},
          metadata: %{"event" => "resumed", "at" => DateTime.to_iso8601(resumed_at)},
          occurred_at: resumed_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [successor_runnable],
          occurred_at: resumed_at
        })
      ])

      assert {:ok, %Snapshot{} = resumed_snapshot} =
               SquidMesh.unblock_run(
                 run_id,
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: resumed_at
               )

      assert resumed_snapshot.status == :running
      assert resumed_snapshot.reason == :attempt_visible

      assert [
               %{step: "record_delivery", status: :available, input: %{account_id: "acct_123"}}
             ] = resumed_snapshot.visible_attempts

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})
      assert Enum.count(run_entries, &(&1.type == :manual_step_resolved)) == 1

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.count(dispatch_entries, &(&1.type == :attempt_scheduled)) == 2
    end

    test "journal runtime does not resolve manual pause after terminal transition" do
      run_id = Ecto.UUID.generate()
      terminal_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{}} =
               SquidMesh.start_run(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-terminal-pause-resolution-test",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      append_read_model_run_entries([
        read_model_entry!(:run_terminal, %{
          run_id: run_id,
          status: :cancelled,
          occurred_at: terminal_at
        })
      ])

      assert {:error, {:invalid_transition, :cancelled, :running}} =
               SquidMesh.unblock_run(
                 run_id,
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: terminal_at
               )

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})
      refute Enum.any?(run_entries, &(&1.type == :manual_step_resolved))
    end

    test "journal runtime recovers built-in pause manual state after dispatch completion" do
      run_id = Ecto.UUID.generate()
      recovery_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{runnable_key: runnable_key, step: "wait_for_approval"}] =
               snapshot.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "pause_claim",
          claim_token_hash: claim_token_hash("pause_token"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 30, :second),
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "pause_claim",
          claim_token_hash: claim_token_hash("pause_token"),
          queue: @read_model_queue,
          result: %{},
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-recovery-test",
                 now: recovery_at
               )

      assert recovered_snapshot.status == :paused
      assert recovered_snapshot.reason == :manual_intervention_required
      assert recovered_snapshot.visible_attempts == []
      assert recovered_snapshot.pending_results == []

      assert recovered_snapshot.manual_state == %{
               step: "wait_for_approval",
               kind: "pause",
               paused_at: @read_model_visible_at,
               metadata: %{
                 output: %{},
                 target: "record_delivery"
               }
             }

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :manual_step_paused
             ]

      assert %{data: %{paused_at: @read_model_visible_at}} = List.last(run_entries)

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-recovery-test",
                 now: recovery_at
               )

      assert {:ok, replayed_run_entries} =
               Journal.load_entries(@read_model_storage, {:run, run_id})

      assert Enum.count(replayed_run_entries, &(&1.type == :manual_step_paused)) == 1
    end

    test "journal runtime recovers built-in approval manual state after dispatch completion" do
      run_id = Ecto.UUID.generate()
      recovery_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 ApprovalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{runnable_key: runnable_key, step: "wait_for_review"}] =
               snapshot.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "approval_claim",
          claim_token_hash: claim_token_hash("approval_token"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 30, :second),
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "approval_claim",
          claim_token_hash: claim_token_hash("approval_token"),
          queue: @read_model_queue,
          result: %{},
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-approval-recovery-test",
                 now: recovery_at
               )

      assert recovered_snapshot.status == :paused
      assert recovered_snapshot.reason == :manual_intervention_required
      assert recovered_snapshot.visible_attempts == []
      assert recovered_snapshot.pending_results == []

      assert recovered_snapshot.manual_state == %{
               step: "wait_for_review",
               kind: "approval",
               paused_at: @read_model_visible_at,
               metadata: %{
                 ok_target: "record_approval",
                 error_target: "record_rejection",
                 output_key: "approval"
               }
             }

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :manual_step_paused
             ]

      refute Enum.any?(
               run_entries,
               &(&1.type == :runnables_planned and
                   Enum.any?(&1.data.runnables, fn runnable ->
                     runnable.step == "record_approval"
                   end))
             )
    end

    test "journal runtime recovers built-in wait successor delay after dispatch completion" do
      run_id = Ecto.UUID.generate()
      delayed_at = DateTime.add(@read_model_visible_at, 2, :second)
      recovery_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 WaitWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{runnable_key: runnable_key}] = snapshot.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "wait_claim",
          claim_token_hash: claim_token_hash("wait_token"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 30, :second),
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "wait_claim",
          claim_token_hash: claim_token_hash("wait_token"),
          queue: @read_model_queue,
          result: %{},
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-wait-recovery-test",
                 now: recovery_at
               )

      assert recovered_snapshot.run_id == run_id
      assert recovered_snapshot.reason == :attempt_scheduled_for_later
      assert recovered_snapshot.visible_attempts == []
      assert recovered_snapshot.next_visible_at == delayed_at

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert [_wait_runnable, delayed_runnable] =
               run_entries
               |> Enum.filter(&(&1.type == :runnables_planned))
               |> Enum.flat_map(&Map.fetch!(&1.data, :runnables))

      assert delayed_runnable.step == "record_settlement"
      assert delayed_runnable.visible_at == delayed_at
    end

    test "journal runtime executes dependency-mode wait steps by delaying dependent successors" do
      run_id = Ecto.UUID.generate()
      delayed_at = DateTime.add(@read_model_visible_at, 4, :second)

      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalDependencyWaitWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert Enum.map(started_snapshot.visible_attempts, & &1.step) == [
               "load_account",
               "load_invoice"
             ]

      assert {:ok, %Snapshot{} = after_account} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-account",
                 claim_id: "claim_account",
                 claim_token: "token_account",
                 now: @read_model_visible_at
               )

      assert after_account.status == :running
      assert Enum.map(after_account.visible_attempts, & &1.step) == ["load_invoice"]

      assert {:ok, %Snapshot{} = after_invoice} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-invoice",
                 claim_id: "claim_invoice",
                 claim_token: "token_invoice",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert after_invoice.status == :running
      assert Enum.map(after_invoice.visible_attempts, & &1.step) == ["wait_for_settlement"]

      assert {:ok, %Snapshot{} = delayed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-wait",
                 claim_id: "claim_wait",
                 claim_token: "token_wait",
                 now: DateTime.add(@read_model_visible_at, 2, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert delayed_snapshot.reason == :attempt_scheduled_for_later
      assert delayed_snapshot.visible_attempts == []
      assert delayed_snapshot.next_visible_at == delayed_at

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-before-delay",
                 now: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-email",
                 claim_id: "claim_email",
                 claim_token: "token_email",
                 now: delayed_at
               )

      assert completed_snapshot.status == :completed
      assert completed_snapshot.reason == :terminal

      assert Enum.map(completed_snapshot.attempts, & &1.step) == [
               "load_account",
               "load_invoice",
               "wait_for_settlement",
               "send_email"
             ]

      assert [%{step: "send_email", input: send_email_input}] =
               Enum.filter(completed_snapshot.attempts, &(&1.step == "send_email"))

      assert send_email_input == %{
               account: %{id: "acct_123"},
               invoice: %{id: "inv_456", status: "open"}
             }

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert [delayed_runnable] =
               run_entries
               |> Enum.filter(&(&1.type == :runnables_planned))
               |> Enum.flat_map(&Map.fetch!(&1.data, :runnables))
               |> Enum.filter(&(&1.step == "send_email"))

      assert delayed_runnable.visible_at == delayed_at
    end

    test "journal runtime recovers dependency wait successor delay after dispatch completion" do
      run_id = Ecto.UUID.generate()
      wait_finished_at = DateTime.add(@read_model_visible_at, 2, :second)
      delayed_at = DateTime.add(wait_finished_at, 2, :second)
      recovery_at = DateTime.add(wait_finished_at, 1, :second)

      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalDependencyWaitWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert Enum.map(started_snapshot.visible_attempts, & &1.step) == [
               "load_account",
               "load_invoice"
             ]

      assert {:ok, %Snapshot{}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-account",
                 claim_id: "claim_account",
                 claim_token: "token_account",
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = after_invoice} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-invoice",
                 claim_id: "claim_invoice",
                 claim_token: "token_invoice",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert [%{runnable_key: runnable_key, step: "wait_for_settlement"}] =
               after_invoice.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_wait",
          claim_token_hash: claim_token_hash("token_wait"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(wait_finished_at, 30, :second),
          occurred_at: wait_finished_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_wait",
          claim_token_hash: claim_token_hash("token_wait"),
          queue: @read_model_queue,
          result: %{},
          occurred_at: wait_finished_at
        })
      ])

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-recovery",
                 now: recovery_at
               )

      assert recovered_snapshot.reason == :attempt_scheduled_for_later
      assert recovered_snapshot.visible_attempts == []
      assert recovered_snapshot.next_visible_at == delayed_at

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert [delayed_runnable] =
               run_entries
               |> Enum.filter(&(&1.type == :runnables_planned))
               |> Enum.flat_map(&Map.fetch!(&1.data, :runnables))
               |> Enum.filter(&(&1.step == "send_email"))

      assert delayed_runnable.visible_at == delayed_at
    end

    test "journal runtime preserves recovered dependency wait delay until later prerequisites finish" do
      run_id = Ecto.UUID.generate()
      wait_finished_at = @read_model_visible_at
      recovery_at = DateTime.add(wait_finished_at, 1, :second)
      delayed_at = DateTime.add(wait_finished_at, 2, :second)

      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalRootWaitWorkflow,
                 %{invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [
               %{runnable_key: runnable_key, step: "wait_for_settlement"},
               %{step: "z_load_invoice"}
             ] =
               started_snapshot.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_wait",
          claim_token_hash: claim_token_hash("token_wait"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(wait_finished_at, 30, :second),
          occurred_at: wait_finished_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_wait",
          claim_token_hash: claim_token_hash("token_wait"),
          queue: @read_model_queue,
          result: %{},
          occurred_at: wait_finished_at
        })
      ])

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "root-wait-recovery",
                 now: recovery_at
               )

      assert recovered_snapshot.status == :running
      assert Enum.map(recovered_snapshot.visible_attempts, & &1.step) == ["z_load_invoice"]

      assert {:ok, %Snapshot{} = delayed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "root-wait-invoice",
                 claim_id: "claim_invoice",
                 claim_token: "token_invoice",
                 now: recovery_at
               )

      assert delayed_snapshot.reason == :attempt_scheduled_for_later
      assert delayed_snapshot.visible_attempts == []
      assert delayed_snapshot.next_visible_at == delayed_at

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert [wait_applied] =
               Enum.filter(
                 run_entries,
                 &(&1.type == :runnable_applied and &1.data.runnable_key == runnable_key)
               )

      assert wait_applied.data.applied_at == wait_finished_at
      assert wait_applied.occurred_at == recovery_at
    end

    test "journal runtime preserves a completed dependency wait delay until later prerequisites finish" do
      run_id = Ecto.UUID.generate()
      delayed_at = DateTime.add(@read_model_visible_at, 2, :second)

      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalRootWaitWorkflow,
                 %{invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert Enum.map(started_snapshot.visible_attempts, & &1.step) == [
               "wait_for_settlement",
               "z_load_invoice"
             ]

      assert {:ok, %Snapshot{} = after_wait} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "root-wait-wait",
                 claim_id: "claim_wait",
                 claim_token: "token_wait",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert after_wait.status == :running
      assert Enum.map(after_wait.visible_attempts, & &1.step) == ["z_load_invoice"]

      assert {:ok, %Snapshot{} = delayed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "root-wait-invoice",
                 claim_id: "claim_invoice",
                 claim_token: "token_invoice",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert delayed_snapshot.reason == :attempt_scheduled_for_later
      assert delayed_snapshot.visible_attempts == []
      assert delayed_snapshot.next_visible_at == delayed_at

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "root-wait-email",
                 claim_id: "claim_email",
                 claim_token: "token_email",
                 now: delayed_at
               )

      assert completed_snapshot.status == :completed

      assert Enum.map(completed_snapshot.attempts, & &1.step) == [
               "wait_for_settlement",
               "z_load_invoice",
               "send_email"
             ]

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert [delayed_runnable] =
               run_entries
               |> Enum.filter(&(&1.type == :runnables_planned))
               |> Enum.flat_map(&Map.fetch!(&1.data, :runnables))
               |> Enum.filter(&(&1.step == "send_email"))

      assert delayed_runnable.visible_at == delayed_at
    end

    test "inspect_run_graph/2 identifies claimed journal attempts as the current node" do
      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: runnable_key}] = snapshot.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: snapshot.run_id,
          runnable_key: runnable_key,
          claim_id: "claim_1",
          claim_token_hash: claim_token_hash("token_1"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 30, :second),
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph} =
               SquidMesh.inspect_run_graph(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      nodes = Map.new(graph.nodes, &{&1.id, &1})

      assert graph.current_node_id == "check_gateway"
      assert graph.current_node_ids == ["check_gateway"]
      assert nodes["check_gateway"].status == :running

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_completed, %{
          run_id: snapshot.run_id,
          runnable_key: runnable_key,
          claim_id: "old_claim",
          claim_token_hash: claim_token_hash("stale_token"),
          queue: @read_model_queue,
          result: %{gateway: %{status: "ok"}},
          occurred_at: DateTime.add(@read_model_visible_at, 1, :second)
        })
      ])

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph_with_anomaly} =
               SquidMesh.inspect_run_graph(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert [%{source: :dispatch, reason: :stale_claim, entry_type: :attempt_completed}] =
               graph_with_anomaly.anomalies

      refute inspect(graph_with_anomaly.anomalies) =~ "old_claim"
      refute inspect(graph_with_anomaly.anomalies) =~ claim_token_hash("stale_token")
    end

    test "journal runtime start can be rebuilt through inspection after process restart" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = rebuilt_snapshot} =
               SquidMesh.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert rebuilt_snapshot.run_id == started_snapshot.run_id
      assert rebuilt_snapshot.workflow == started_snapshot.workflow
      assert rebuilt_snapshot.thread_revisions == started_snapshot.thread_revisions
      assert rebuilt_snapshot.visible_attempts == started_snapshot.visible_attempts
      assert rebuilt_snapshot.pending_dispatches == []
    end

    test "journal runtime start infers Ecto journal storage from the configured repo" do
      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal
               )

      assert snapshot.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert snapshot.status == :running
    end

    test "table runtime start rejects journal-only options" do
      assert {:error, {:invalid_option, {:runtime_tables, [:journal_storage]}}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :runtime_tables,
                 journal_storage: @read_model_storage
               )
    end

    test "start_run/3 redacts invalid runtime values" do
      assert {:error, reason} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: %{claim_token: "super-secret-token"}
               )

      assert reason == {:invalid_option, {:runtime, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "journal runtime start rejects malformed public options" do
      assert {:error, {:invalid_option, {:journal_storage, String}}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: String
               )

      assert {:error, {:invalid_option, {:journal_storage, Jido.Storage.File}}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: {Jido.Storage.File, []}
               )

      assert {:error, {:invalid_option, {:journal_storage, String}}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: {String, path: "/tmp/squid_mesh_storage", token: "redacted"}
               )

      assert {:error, {:invalid_option, {:queue, :invalid}}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "../dispatch"
               )

      assert {:error, {:invalid_option, {:run_id, :invalid}}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 run_id: "not-a-uuid"
               )

      assert {:error, reason} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 now: %{claim_token: "super-secret-token"}
               )

      assert reason == {:invalid_option, {:now, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "journal runtime start reports committed run id after post-append failures" do
      run_id = Ecto.UUID.generate()

      assert {:error, {:journal_start_committed, ^run_id, :load_failed}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: CommitThenFailStorage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )
    end

    test "journal runtime start idempotently repairs duplicate caller-provided run ids" do
      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert snapshot.run_id == run_id

      assert {:ok, %Snapshot{} = duplicate_snapshot} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert duplicate_snapshot.run_id == run_id

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})
      assert Enum.map(run_entries, & &1.type) == [:run_started, :runnables_planned]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.count(dispatch_entries, &(&1.type == :attempt_scheduled)) == 1
    end

    test "journal runtime start rejects duplicate run ids with conflicting planned work" do
      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:error, :conflict} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )
    end

    test "journal runtime start rejects duplicate run ids with conflicting definition fingerprints" do
      run_id = Ecto.UUID.generate()

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          definition_fingerprint: "stale-definition",
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        })
      ])

      assert {:error, :conflict} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:error, :not_found} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})
    end

    test "journal runtime start repairs a partially appended run thread" do
      run_id = Ecto.UUID.generate()
      assert {:ok, definition} = Definition.load(PaymentRecoveryWorkflow)

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 30, :second),
                 run_id: run_id
               )

      assert snapshot.run_id == run_id
      assert snapshot.pending_dispatches == []

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [:run_queued, :attempt_scheduled]

      assert {:ok, run_index_projection} =
               Journal.rebuild_run_index_projection(
                 @read_model_storage,
                 Atom.to_string(PaymentRecoveryWorkflow)
               )

      assert SquidMesh.Runtime.RunIndexProjection.run_ids(run_index_projection) == [run_id]
    end

    test "journal runtime start retries same-queue dispatch append conflicts" do
      warm_read_model_storage()

      results =
        1..8
        |> Task.async_stream(
          fn index ->
            SquidMesh.start_run(
              PaymentRecoveryWorkflow,
              %{account_id: "acct_#{index}"},
              runtime: :journal,
              journal_storage: @read_model_storage,
              queue: @read_model_queue,
              now: DateTime.add(@read_model_started_at, index, :second),
              run_id: Ecto.UUID.generate()
            )
          end,
          max_concurrency: 8,
          timeout: 5_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:ok, %Snapshot{}}, &1))

      started_run_ids =
        Enum.map(results, fn {:ok, %Snapshot{} = snapshot} -> snapshot.run_id end)

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      scheduled_run_ids =
        dispatch_entries
        |> Enum.filter(&(&1.type == :attempt_scheduled))
        |> Enum.map(& &1.data.run_id)
        |> Enum.sort()

      assert scheduled_run_ids == Enum.sort(started_run_ids)
    end

    test "execute_next/1 runs and applies one visible journal attempt without writing legacy runtime tables" do
      legacy_counts_before = legacy_runtime_counts()

      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = executed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert executed_snapshot.run_id == started_snapshot.run_id
      assert executed_snapshot.reason == :terminal
      assert executed_snapshot.status == :completed
      assert executed_snapshot.terminal? == true
      assert executed_snapshot.terminal_status == :completed
      assert executed_snapshot.visible_attempts == []
      assert executed_snapshot.pending_results == []
      assert executed_snapshot.applied_runnable_keys == started_snapshot.planned_runnable_keys

      assert [
               %{
                 status: :completed,
                 step: "check_gateway",
                 result: %{gateway_check: %{account_id: "acct_123", status: "healthy"}},
                 applied?: true
               }
             ] = executed_snapshot.attempts

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_completed
             ]

      assert legacy_runtime_counts() == legacy_counts_before
    end

    test "execute_next/1 plans and schedules the successor step after a journal completion" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = progressed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert progressed_snapshot.run_id == started_snapshot.run_id
      assert progressed_snapshot.status == :running
      assert progressed_snapshot.reason == :attempt_visible

      assert [%{step: "send_email", status: :available, input: successor_input}] =
               progressed_snapshot.visible_attempts

      assert successor_input == %{
               account_id: "acct_123",
               invoice_id: "inv_456",
               account: %{id: "acct_123"},
               invoice: %{id: "inv_456", status: "open"}
             }

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert completed_snapshot.status == :completed
      assert completed_snapshot.reason == :terminal
      assert completed_snapshot.terminal? == true

      assert Enum.map(completed_snapshot.attempts, & &1.step) == [
               "load_invoice",
               "send_email"
             ]
    end

    test "execute_next/1 plans the journal successor selected by a transition condition" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalConditionalWorkflow,
                 %{account_id: "acct_123", decision: "auto"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = progressed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert progressed_snapshot.run_id == started_snapshot.run_id
      assert progressed_snapshot.status == :running

      assert [%{step: "auto_approve", status: :available, input: successor_input}] =
               progressed_snapshot.visible_attempts

      assert successor_input.routing == %{decision: "auto"}

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert %{
               transition: %{
                 "from" => "classify",
                 "on" => "ok",
                 "to" => "auto_approve",
                 "condition" => %{"path" => ["routing", "decision"], "equals" => "auto"}
               }
             } =
               run_entries
               |> Enum.find(&(&1.type == :runnable_applied))
               |> then(& &1.data)

      assert {:ok, graph} =
               SquidMesh.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert %{
               {"classify", "auto_approve"} => :selected,
               {"classify", "manual_review"} => :skipped
             } =
               graph.edges
               |> Enum.filter(&(&1.from == "classify"))
               |> Map.new(&{{&1.from, &1.to}, &1.status})

      auto_edge = Enum.find(graph.edges, &(&1.from == "classify" and &1.to == "auto_approve"))
      assert auto_edge.condition == %{path: [:routing, :decision], equals: "auto"}
    end

    test "execute_next/1 evaluates journal conditions against accumulated context" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalAccumulatedConditionalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{reason: :attempt_visible}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = branched_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert [%{step: "auto_approve", status: :available, input: successor_input}] =
               branched_snapshot.visible_attempts

      assert successor_input.profile == %{account_id: "acct_123", tier: "trusted"}

      assert {:ok, graph} =
               SquidMesh.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert %{
               {"classify", "auto_approve"} => :selected,
               {"classify", "manual_review"} => :skipped
             } =
               graph.edges
               |> Enum.filter(&(&1.from == "classify"))
               |> Map.new(&{{&1.from, &1.to}, &1.status})
    end

    test "execute_next/1 records selected conditional error transitions to complete" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalConditionalErrorCompleteWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert completed_snapshot.run_id == started_snapshot.run_id
      assert completed_snapshot.status == :completed

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]

      assert %{
               transition: %{
                 "from" => "fail_gateway",
                 "on" => "error",
                 "to" => "__complete__",
                 "condition" => %{"path" => ["account_id"], "equals" => "acct_123"}
               }
             } =
               run_entries
               |> Enum.find(&(&1.type == :runnable_applied))
               |> then(& &1.data)
    end

    test "execute_next/1 skips conditional error completion after a terminal conflict" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalConditionalErrorCompleteWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      parent = self()

      :persistent_term.put(:journal_conditional_error_complete_conflict_hook, fn %{run_id: run_id} ->
        assert run_id == started_snapshot.run_id
        send(parent, :conditional_error_complete_conflict_hook_called)

        append_read_model_run_entries([
          read_model_entry!(:run_terminal, %{
            run_id: run_id,
            status: :cancelled,
            occurred_at: @read_model_visible_at
          })
        ])
      end)

      try do
        assert {:error, :terminal_run} =
                 execute_journal_next(
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   owner_id: "worker_1",
                   claim_id: "claim_1",
                   claim_token: "token_1",
                   now: @read_model_visible_at
                 )

        assert_receive :conditional_error_complete_conflict_hook_called
      after
        :persistent_term.erase(:journal_conditional_error_complete_conflict_hook)
      end

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :run_terminal
             ]
    end

    test "execute_next/1 advances dependency workflows after prerequisites complete" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalDependencyWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert Enum.map(started_snapshot.visible_attempts, & &1.step) == [
               "load_account",
               "load_invoice"
             ]

      assert {:ok, %Snapshot{} = after_account} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert after_account.status == :running
      assert Enum.map(after_account.visible_attempts, & &1.step) == ["load_invoice"]

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph} =
               SquidMesh.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      nodes = Map.new(graph.nodes, &{&1.id, &1})
      edges = Map.new(graph.edges, &{&1.id, &1})

      assert graph.source == :read_model
      assert nodes["load_account"].status == :completed
      assert nodes["load_invoice"].status == :pending
      assert nodes["send_email"].status == :waiting
      assert edges["load_account:dependency:send_email"].status == :selected
      assert edges["load_invoice:dependency:send_email"].status == :pending

      assert {:ok, %Snapshot{} = after_invoice} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert after_invoice.status == :running
      assert [%{step: "send_email", input: send_email_input}] = after_invoice.visible_attempts

      assert send_email_input == %{
               account: %{id: "acct_123"},
               invoice: %{id: "inv_456", status: "open"}
             }

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_3",
                 claim_id: "claim_3",
                 claim_token: "token_3",
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert completed_snapshot.status == :completed
      assert completed_snapshot.reason == :terminal

      assert Enum.map(completed_snapshot.attempts, & &1.step) == [
               "load_account",
               "load_invoice",
               "send_email"
             ]
    end

    test "execute_next/1 fails journal runs durably when successor named path input is missing" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalMissingPathWorkflow,
                 %{draft: %{}},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = failed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert failed_snapshot.run_id == started_snapshot.run_id
      assert failed_snapshot.status == :failed
      assert failed_snapshot.reason == :terminal
      assert failed_snapshot.terminal? == true
      assert failed_snapshot.visible_attempts == []

      assert [
               %{
                 step: "load_review_context",
                 status: :completed,
                 result: %{draft: %{}}
               }
             ] = failed_snapshot.attempts

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]

      assert %{
               error: %{
                 code: "missing_input_path",
                 path: ["draft", "drafts"],
                 target: "drafts",
                 missing_at: ["draft", "drafts"]
               }
             } = List.last(run_entries).data

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_completed
             ]
    end

    test "execute_next/1 recovers completed successor mapping failures with terminal error history" do
      run_id = Ecto.UUID.generate()
      assert {:ok, definition} = Definition.load(JournalMissingPathWorkflow)
      runnable = journal_missing_path_runnable(run_id)
      claim_token_hash = claim_token_hash("token_1")

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(JournalMissingPathWorkflow),
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [runnable],
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_dispatch_entries([
        read_model_entry!(:run_queued, %{
          run_id: run_id,
          queue: @read_model_queue,
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(
          :attempt_scheduled,
          Map.put(runnable, :occurred_at, @read_model_started_at)
        ),
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable.runnable_key,
          claim_id: "claim_1",
          claim_token_hash: claim_token_hash,
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 300, :second),
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable.runnable_key,
          claim_id: "claim_1",
          claim_token_hash: claim_token_hash,
          queue: @read_model_queue,
          result: %{draft: %{}},
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert snapshot.status == :failed
      assert snapshot.reason == :terminal

      assert [
               %{
                 step: "load_review_context",
                 status: :completed,
                 result: %{draft: %{}}
               }
             ] = snapshot.attempts

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]

      assert %{
               error: %{
                 code: "missing_input_path",
                 path: ["draft", "drafts"],
                 target: "drafts",
                 missing_at: ["draft", "drafts"]
               }
             } = List.last(run_entries).data
    end

    test "execute_next/1 recomputes dependency progress after concurrent root append conflicts" do
      on_exit(fn -> :persistent_term.erase(:journal_dependency_invoice_hook) end)

      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalDependencyWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{attempt: account_attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert account_attempt.step == "load_account"

      :persistent_term.put(:journal_dependency_invoice_hook, fn ->
        account_result = %{account: %{id: "acct_123"}}

        assert {:ok, latest_dispatch_agent} =
                 DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

        assert {:ok, %{}} =
                 DispatchAgent.complete(
                   @read_model_storage,
                   latest_dispatch_agent,
                   account_attempt.runnable_key,
                   "claim_1",
                   "token_1",
                   account_result,
                   now: DateTime.add(@read_model_visible_at, 1, :millisecond)
                 )

        append_read_model_run_entries([
          read_model_entry!(:runnable_applied, %{
            run_id: started_snapshot.run_id,
            runnable_key: account_attempt.runnable_key,
            result: account_result,
            occurred_at: DateTime.add(@read_model_visible_at, 1, :millisecond)
          })
        ])
      end)

      assert {:ok, %Snapshot{} = after_invoice} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert after_invoice.status == :running
      assert [%{step: "send_email", input: send_email_input}] = after_invoice.visible_attempts

      assert send_email_input == %{
               account: %{id: "acct_123"},
               invoice: %{id: "inv_456", status: "open"}
             }

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :runnable_applied,
               :runnables_planned
             ]
    end

    test "execute_next/1 terminally fails dependency workflows after nonretryable root failure" do
      assert {:ok, %Snapshot{}} =
               SquidMesh.start_run(
                 JournalDependencyFailureWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert snapshot.status == :failed
      assert snapshot.reason == :terminal
      assert snapshot.terminal? == true
      assert snapshot.visible_attempts == []

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :run_terminal
             ]
    end

    test "execute_next/1 returns none after the visible journal attempt is already applied" do
      assert {:ok, %Snapshot{}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_completed
             ]
    end

    test "execute_next/1 recovers a completed attempt that crashed before run progression" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %{}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 %{gateway_check: %{account_id: "acct_123", status: "healthy"}},
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{reason: :completed_result_pending_apply}} =
               SquidMesh.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert recovered_snapshot.status == :completed
      assert recovered_snapshot.reason == :terminal
      assert recovered_snapshot.applied_runnable_keys == started_snapshot.planned_runnable_keys

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]
    end

    test "execute_next/1 does not apply completed attempts after the run became terminal" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %{}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 %{gateway_check: %{account_id: "acct_123", status: "healthy"}},
                 now: @read_model_visible_at
               )

      append_read_model_run_entries([
        read_model_entry!(:run_terminal, %{
          run_id: started_snapshot.run_id,
          status: :cancelled,
          occurred_at: DateTime.add(@read_model_visible_at, 1, :second)
        })
      ])

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :run_terminal
             ]
    end

    test "execute_next/1 recovers a failed attempt that crashed before run progression" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalFailureWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %{}} =
               DispatchAgent.fail(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 %{code: "gateway_timeout", message: "gateway timeout", retryable?: false},
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{reason: :waiting_for_dispatch}} =
               SquidMesh.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert recovered_snapshot.status == :failed
      assert recovered_snapshot.reason == :terminal

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :run_terminal
             ]
    end

    test "execute_next/1 recovers dispatch scheduling after run progression was committed" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      result = %{
        account: %{id: "acct_123"},
        invoice: %{id: "inv_456", status: "open"}
      }

      assert {:ok, %{}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 result,
                 now: @read_model_visible_at
               )

      successor_runnable = %{
        run_id: started_snapshot.run_id,
        runnable_key: "#{started_snapshot.run_id}:send_email:1",
        idempotency_key: "#{started_snapshot.run_id}:send_email:1",
        attempt_number: 1,
        queue: @read_model_queue,
        step: "send_email",
        input: result,
        visible_at: @read_model_visible_at
      }

      append_read_model_run_entries([
        read_model_entry!(:runnable_applied, %{
          run_id: started_snapshot.run_id,
          runnable_key: attempt.runnable_key,
          result: result,
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: started_snapshot.run_id,
          runnables: [successor_runnable],
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{reason: :planned_dispatch_pending_schedule}} =
               SquidMesh.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert recovered_snapshot.reason == :attempt_visible
      assert Enum.map(recovered_snapshot.visible_attempts, & &1.step) == ["send_email"]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.count(dispatch_entries, &(&1.type == :attempt_scheduled)) == 2
    end

    test "execute_next/1 recovers initial dispatch scheduling from a queued run marker" do
      run_id = Ecto.UUID.generate()
      runnable = journal_start_runnable(run_id)
      assert {:ok, definition} = Definition.load(PaymentRecoveryWorkflow)

      append_read_model_dispatch_entries([
        read_model_entry!(:run_queued, %{
          run_id: run_id,
          queue: @read_model_queue,
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Definition.serialize_workflow(PaymentRecoveryWorkflow),
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [runnable],
          occurred_at: @read_model_started_at
        })
      ])

      assert {:ok, %Snapshot{reason: :planned_dispatch_pending_schedule}} =
               SquidMesh.inspect_run(run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert recovered_snapshot.reason == :attempt_visible

      assert [%{runnable_key: runnable_key, step: "check_gateway", status: :available}] =
               recovered_snapshot.visible_attempts

      assert runnable_key == runnable.runnable_key

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [:run_queued, :attempt_scheduled]
    end

    test "execute_next/1 ignores queued run markers for runs planned on another queue" do
      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{run_id: ^run_id}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "default",
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:error, :conflict} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "other",
                 now: DateTime.add(@read_model_started_at, 1, :second),
                 run_id: run_id
               )

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "other",
                 owner_id: "worker_1",
                 now: @read_model_visible_at
               )
    end

    test "execute_next/1 does not repeatedly recover failed attempts after an error transition is planned" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalErrorTransitionWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %{}} =
               DispatchAgent.fail(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 %{code: "gateway_timeout", message: "gateway timeout", retryable?: false},
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{reason: :attempt_visible} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert Enum.map(recovered_snapshot.visible_attempts, & &1.step) == ["notify_failure"]

      assert {:ok, graph} =
               SquidMesh.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert %{status: :selected} =
               Enum.find(
                 graph.edges,
                 &(&1.from == "fail_gateway" and &1.to == "notify_failure")
               )

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_3",
                 claim_id: "claim_3",
                 claim_token: "token_3",
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert completed_snapshot.status == :completed
      assert completed_snapshot.reason == :terminal

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]
    end

    test "execute_next/1 does not duplicate error transition progression after a run-thread conflict" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalErrorTransitionWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      notify_runnable = %{
        run_id: started_snapshot.run_id,
        runnable_key: "#{started_snapshot.run_id}:notify_failure:1",
        idempotency_key: "#{started_snapshot.run_id}:notify_failure:1",
        attempt_number: 1,
        queue: @read_model_queue,
        step: "notify_failure",
        input: %{},
        visible_at: @read_model_visible_at
      }

      failed_runnable_key = "#{started_snapshot.run_id}:fail_gateway:1"
      parent = self()

      :persistent_term.put(:journal_error_transition_conflict_hook, fn %{run_id: run_id} ->
        assert run_id == started_snapshot.run_id
        send(parent, :error_transition_conflict_hook_called)

        append_read_model_run_entries([
          read_model_entry!(:runnable_applied, %{
            run_id: run_id,
            runnable_key: failed_runnable_key,
            result: %{},
            occurred_at: @read_model_visible_at
          }),
          read_model_entry!(:runnables_planned, %{
            run_id: run_id,
            runnables: [notify_runnable],
            occurred_at: @read_model_visible_at
          })
        ])
      end)

      try do
        assert {:ok, %Snapshot{} = snapshot} =
                 execute_journal_next(
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   owner_id: "worker_1",
                   claim_id: "claim_1",
                   claim_token: "token_1",
                   now: @read_model_visible_at
                 )

        assert_receive :error_transition_conflict_hook_called
        assert Enum.map(snapshot.visible_attempts, & &1.step) == ["notify_failure"]
      after
        :persistent_term.erase(:journal_error_transition_conflict_hook)
      end

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.count(run_entries, &(&1.type == :runnable_applied)) == 1

      notify_plan_count =
        Enum.count(run_entries, fn
          %{type: :runnables_planned, data: %{runnables: [%{runnable_key: key}]}} ->
            key == notify_runnable.runnable_key

          _entry ->
            false
        end)

      assert notify_plan_count == 1
    end

    test "execute_next/1 fails an incompatible claimed attempt durably" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:missing_gateway:1"
      assert {:ok, definition} = Definition.load(PaymentRecoveryWorkflow)

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [
            %{
              run_id: run_id,
              runnable_key: runnable_key,
              idempotency_key: runnable_key,
              attempt_number: 1,
              queue: @read_model_queue,
              step: "missing_gateway",
              input: %{account_id: "acct_123"},
              visible_at: @read_model_started_at
            }
          ],
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_scheduled, %{
          run_id: run_id,
          runnable_key: runnable_key,
          idempotency_key: runnable_key,
          attempt_number: 1,
          queue: @read_model_queue,
          step: "missing_gateway",
          input: %{account_id: "acct_123"},
          visible_at: @read_model_started_at,
          occurred_at: @read_model_started_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert snapshot.status == :failed
      assert snapshot.reason == :terminal

      assert [
               %{
                 status: :failed,
                 step: "missing_gateway",
                 error: %{
                   message: "journal attempt is incompatible with the current workflow definition"
                 }
               }
             ] = snapshot.attempts

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})
      assert Enum.map(run_entries, & &1.type) == [:run_started, :runnables_planned, :run_terminal]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_failed
             ]
    end

    test "execute_next/1 rejects missing workflow definition fingerprints before executing" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:check_gateway:1"

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_scheduled, %{
          run_id: run_id,
          runnable_key: runnable_key,
          idempotency_key: runnable_key,
          attempt_number: 1,
          queue: @read_model_queue,
          step: "check_gateway",
          input: %{account_id: "acct_123"},
          visible_at: @read_model_started_at,
          occurred_at: @read_model_started_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert snapshot.status == :failed

      assert [%{status: :failed, error: %{code: "incompatible_workflow_definition"}}] =
               snapshot.attempts
    end

    test "execute_next/1 rejects stale workflow definition fingerprints before executing" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:check_gateway:1"

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          definition_fingerprint: "stale-definition",
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_scheduled, %{
          run_id: run_id,
          runnable_key: runnable_key,
          idempotency_key: runnable_key,
          attempt_number: 1,
          queue: @read_model_queue,
          step: "check_gateway",
          input: %{account_id: "acct_123"},
          visible_at: @read_model_started_at,
          occurred_at: @read_model_started_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert snapshot.status == :failed

      assert [%{status: :failed, error: %{code: "incompatible_workflow_definition"}}] =
               snapshot.attempts
    end

    test "execute_next/1 terminally fails stale completed attempts during recovery" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:check_gateway:1"

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          definition_fingerprint: "stale-definition",
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_scheduled, %{
          run_id: run_id,
          runnable_key: runnable_key,
          idempotency_key: runnable_key,
          attempt_number: 1,
          queue: @read_model_queue,
          step: "check_gateway",
          input: %{account_id: "acct_123"},
          visible_at: @read_model_started_at,
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_1",
          claim_token_hash: "token_hash_1",
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 300, :second),
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_1",
          claim_token_hash: "token_hash_1",
          queue: @read_model_queue,
          result: %{gateway_check: %{account_id: "acct_123", status: "healthy"}},
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert snapshot.status == :failed
      assert snapshot.reason == :terminal

      assert {:ok, run_entries} = Journal.load_entries(@read_model_storage, {:run, run_id})
      assert Enum.map(run_entries, & &1.type) == [:run_started, :runnables_planned, :run_terminal]
    end

    test "execute_next/1 uses completion time for lease fencing" do
      assert {:ok, %Snapshot{}} =
               SquidMesh.start_run(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:error, :expired_claim} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 lease_for: 1,
                 now: @read_model_visible_at,
                 finished_at: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed
             ]
    end

    test "execute_next/1 retries terminal append after unrelated same-queue writes" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalConflictWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      :persistent_term.put(:journal_executor_conflict_hook, fn ->
        assert {:ok, %Snapshot{}} =
                 SquidMesh.start_run(
                   PaymentRecoveryWorkflow,
                   %{account_id: "acct_456"},
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   now: DateTime.add(@read_model_started_at, 1, :second),
                   run_id: Ecto.UUID.generate()
                 )
      end)

      try do
        assert {:ok, %Snapshot{} = snapshot} =
                 execute_journal_next(
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   owner_id: "worker_1",
                   claim_id: "claim_1",
                   claim_token: "token_1",
                   now: @read_model_visible_at
                 )

        assert snapshot.run_id == started_snapshot.run_id
        assert snapshot.status == :completed
        assert snapshot.reason == :terminal
      after
        :persistent_term.erase(:journal_executor_conflict_hook)
      end

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :run_queued,
               :attempt_scheduled,
               :attempt_completed
             ]
    end

    test "execute_next/1 records durable failed-attempt facts" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalFailureWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = executed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert executed_snapshot.run_id == started_snapshot.run_id
      assert executed_snapshot.reason == :terminal
      assert executed_snapshot.status == :failed
      assert executed_snapshot.terminal? == true
      assert executed_snapshot.terminal_status == :failed
      assert executed_snapshot.applied_runnable_keys == []

      assert [
               %{
                 status: :failed,
                 step: "fail_gateway",
                 error: %{
                   code: "gateway_timeout",
                   message: "gateway timeout",
                   retryable?: false
                 }
               }
             ] = executed_snapshot.attempts

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :run_terminal
             ]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_failed
             ]
    end

    test "execute_next/1 schedules retry attempts through the journal dispatch projection" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalRetryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert snapshot.run_id == started_snapshot.run_id
      assert snapshot.status == :running
      assert snapshot.reason == :attempt_visible
      assert snapshot.terminal? == false

      assert [
               %{status: :failed, step: "retry_gateway", error: %{retryable?: true}},
               %{status: :retry_scheduled, step: "retry_gateway", attempt_number: 2}
             ] = snapshot.attempts

      assert [%{status: :retry_scheduled, attempt_number: 2}] = snapshot.visible_attempts

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph} =
               SquidMesh.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      nodes = Map.new(graph.nodes, &{&1.id, &1})
      edges = Map.new(graph.edges, &{&1.id, &1})

      assert nodes["retry_gateway"].status == :retrying
      assert nodes["retry_gateway"].attempts == []
      assert edges["retry_gateway:ok:complete"].status == :pending

      assert {:ok, %SquidMesh.Runs.GraphInspection{} = graph_with_history} =
               SquidMesh.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 include_history: true
               )

      nodes_with_history = Map.new(graph_with_history.nodes, &{&1.id, &1})

      assert [
               %{status: :failed, attempt_number: 1},
               %{status: :retry_scheduled, attempt_number: 2}
             ] = nodes_with_history["retry_gateway"].attempts

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_failed
             ]

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnables_planned
             ]

      assert {:ok, %Snapshot{} = exhausted_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert exhausted_snapshot.status == :failed
      assert exhausted_snapshot.reason == :terminal

      assert Enum.map(exhausted_snapshot.attempts, &{&1.status, &1.attempt_number}) == [
               {:failed, 1},
               {:failed, 2}
             ]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_failed,
               :attempt_claimed,
               :attempt_failed
             ]
    end

    test "execute_next/1 does not duplicate retry progression after a run-thread conflict" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               SquidMesh.start_run(
                 JournalRetryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      retry_runnable = %{
        run_id: started_snapshot.run_id,
        runnable_key: "#{started_snapshot.run_id}:retry_gateway:2",
        idempotency_key: "#{started_snapshot.run_id}:retry_gateway:2",
        attempt_number: 2,
        queue: @read_model_queue,
        step: "retry_gateway",
        input: %{account_id: "acct_123"},
        visible_at: @read_model_visible_at
      }

      parent = self()

      :persistent_term.put(:journal_retry_failure_conflict_hook, fn %{run_id: run_id} ->
        assert run_id == started_snapshot.run_id
        send(parent, :retry_failure_conflict_hook_called)

        append_read_model_run_entries([
          read_model_entry!(:runnables_planned, %{
            run_id: run_id,
            runnables: [retry_runnable],
            occurred_at: @read_model_visible_at
          })
        ])
      end)

      try do
        assert {:ok, %Snapshot{} = snapshot} =
                 execute_journal_next(
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   owner_id: "worker_1",
                   claim_id: "claim_1",
                   claim_token: "token_1",
                   now: @read_model_visible_at
                 )

        assert_receive :retry_failure_conflict_hook_called
        assert [%{status: :retry_scheduled, attempt_number: 2}] = snapshot.visible_attempts
      after
        :persistent_term.erase(:journal_retry_failure_conflict_hook)
      end

      assert {:ok, run_entries} =
               Journal.load_entries(@read_model_storage, {:run, started_snapshot.run_id})

      retry_plan_count =
        Enum.count(run_entries, fn
          %{type: :runnables_planned, data: %{runnables: [%{runnable_key: key}]}} ->
            key == retry_runnable.runnable_key

          _entry ->
            false
        end)

      assert retry_plan_count == 1
    end

    test "execute_next/1 redacts secret-bearing action errors before persistence" do
      assert {:ok, %Snapshot{}} =
               SquidMesh.start_run(
                 JournalSecretFailureWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      {snapshot, log} =
        with_log(fn ->
          assert {:ok, %Snapshot{} = snapshot} =
                   execute_journal_next(
                     runtime: :journal,
                     journal_storage: @read_model_storage,
                     queue: @read_model_queue,
                     owner_id: "worker_1",
                     claim_id: "claim_1",
                     claim_token: "token_1",
                     now: @read_model_visible_at
                   )

          snapshot
        end)

      assert [%{error: error}] = snapshot.attempts

      assert error == %{
               code: "step_error",
               message: "step execution failed",
               retryable?: false
             }

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      failed_entry = Enum.find(dispatch_entries, &(&1.type == :attempt_failed))

      refute log =~ "super-secret-token"
      refute inspect(failed_entry.data.error) =~ "super-secret-token"
      refute inspect(snapshot) =~ "super-secret-token"
    end

    test "execute_next/1 rejects malformed option lists without leaking claim tokens" do
      assert {:error, reason} =
               execute_journal_next([{:claim_token, "super-secret-token"}, :not_a_pair])

      assert reason == {:invalid_option, {:opts, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "execute_next/1 rejects non-list options without leaking claim tokens" do
      assert {:error, reason} = execute_journal_next(%{claim_token: "super-secret-token"})

      assert reason == {:invalid_option, {:opts, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "public execute_next/1 rejects internal executor controls" do
      for option <- [:claim_id, :claim_token, :finished_at] do
        assert {:error, {:invalid_option, {:option, ^option}}} =
                 SquidMesh.execute_next(
                   Keyword.put(
                     [
                       runtime: :journal,
                       journal_storage: @read_model_storage
                     ],
                     option,
                     "internal"
                   )
                 )
      end
    end

    test "execute_next/1 redacts invalid option values" do
      secret_value = %{claim_token: "super-secret-token"}

      assert {:error, reason} =
               execute_journal_next(runtime: :journal, finished_at: secret_value)

      assert reason == {:invalid_option, {:finished_at, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 now: secret_value
               )

      assert reason == {:invalid_option, {:now, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: secret_value
               )

      assert reason == {:invalid_option, {:queue, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 owner_id: secret_value
               )

      assert reason == {:invalid_option, {:owner_id, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "explain_run/2 can read from the read model" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      assert {:ok, %Diagnostic{} = explanation} =
               SquidMesh.explain_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert explanation.run_id == @read_model_run_id
      assert explanation.workflow == @read_model_workflow
      assert explanation.queue == @read_model_queue
      assert explanation.reason == :planned_dispatch_pending_schedule
      assert explanation.next_actions == [:schedule_pending_dispatch]
    end

    test "read model infers Ecto journal storage from the configured repo" do
      assert {:error, :not_found} =
               SquidMesh.inspect_run(@read_model_run_id, read_model: :read_model)

      assert {:error, :not_found} =
               SquidMesh.explain_run(@read_model_run_id, read_model: :read_model)
    end

    test "read model rejects malformed journal storage without leaking options" do
      assert {:error, {:invalid_option, {:journal_storage, Jido.Storage.File}}} =
               SquidMesh.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: {Jido.Storage.File, []}
               )

      assert {:error, {:invalid_option, {:journal_storage, String}}} =
               SquidMesh.explain_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: {String, path: "/tmp/squid_mesh_storage", token: "redacted"}
               )
    end

    test "returns a structured error for unsupported read models" do
      assert {:error, {:invalid_option, {:read_model, :invalid}}} =
               SquidMesh.inspect_run(@read_model_run_id, read_model: :unknown)

      assert {:error, {:invalid_option, {:read_model, :invalid}}} =
               SquidMesh.explain_run(@read_model_run_id, read_model: :unknown)
    end

    test "read model APIs redact invalid read_model values" do
      assert {:error, reason} =
               SquidMesh.inspect_run(@read_model_run_id,
                 read_model: %{claim_token: "super-secret-token"}
               )

      assert reason == {:invalid_option, {:read_model, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               SquidMesh.explain_run(@read_model_run_id,
                 read_model: %{claim_token: "super-secret-token"}
               )

      assert reason == {:invalid_option, {:read_model, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "returns a structured error for malformed option lists" do
      assert {:error, {:invalid_option, {:opts, :invalid}}} =
               SquidMesh.inspect_run(@read_model_run_id, [:bad])

      assert {:error, {:invalid_option, {:opts, :invalid}}} =
               SquidMesh.explain_run(@read_model_run_id, [:bad])
    end

    test "read model APIs reject malformed options without leaking claim tokens" do
      assert {:error, reason} =
               SquidMesh.inspect_run(@read_model_run_id, %{
                 read_model: :read_model,
                 claim_token: "super-secret-token"
               })

      assert reason == {:invalid_option, {:opts, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               SquidMesh.explain_run(@read_model_run_id, [
                 {:read_model, :read_model},
                 {:claim_token, "super-secret-token"},
                 :not_a_pair
               ])

      assert reason == {:invalid_option, {:opts, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "read model rejects malformed run ids without raising" do
      assert {:error, {:invalid_option, {:run_id, :invalid}}} =
               SquidMesh.inspect_run(123,
                 read_model: :read_model,
                 journal_storage: @read_model_storage
               )

      assert {:error, {:invalid_option, {:run_id, :invalid}}} =
               SquidMesh.explain_run(123,
                 read_model: :read_model,
                 journal_storage: @read_model_storage
               )
    end

    test "read model rejects storage-unsafe run ids and queues" do
      assert {:error, {:invalid_option, {:run_id, :invalid}}} =
               SquidMesh.inspect_run("../run",
                 read_model: :read_model,
                 journal_storage: @read_model_storage
               )

      assert {:error, {:invalid_option, {:queue, :invalid}}} =
               SquidMesh.explain_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: "../dispatch"
               )
    end

    test "read model redacts invalid option values" do
      secret_value = %{claim_token: "super-secret-token"}

      assert {:error, reason} =
               SquidMesh.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 now: secret_value
               )

      assert reason == {:invalid_option, {:now, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               SquidMesh.explain_run(secret_value,
                 read_model: :read_model,
                 journal_storage: @read_model_storage
               )

      assert reason == {:invalid_option, {:run_id, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               SquidMesh.explain_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: secret_value
               )

      assert reason == {:invalid_option, {:queue, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end
  end

  defp append_read_model_run_entries(entries) do
    assert {:ok, _thread} = Journal.append_entries(@read_model_storage, entries)
  end

  defp append_read_model_dispatch_entries(entries) do
    assert {:ok, _thread} = Journal.append_entries(@read_model_storage, entries)
  end

  defp warm_read_model_storage do
    assert {:ok, seed_entry} =
             DispatchProtocol.new_entry(:run_indexed, %{
               run_id: "storage_seed",
               workflow: "StorageSeedWorkflow",
               queue: @read_model_queue,
               occurred_at: @read_model_started_at
             })

    assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [seed_entry])

    assert :ok =
             Journal.put_checkpoint(
               @read_model_storage,
               {:run_index, "StorageSeedWorkflow"},
               SquidMesh.Runtime.RunIndexProjection.new("StorageSeedWorkflow"),
               1
             )
  end

  defp legacy_runtime_counts do
    %{
      runs: Repo.aggregate(SquidMesh.Persistence.Run, :count, :id),
      step_runs: Repo.aggregate(SquidMesh.Persistence.StepRun, :count, :id),
      step_attempts: Repo.aggregate(SquidMesh.Persistence.StepAttempt, :count, :id)
    }
  end

  defp read_model_run_started do
    read_model_entry!(:run_started, %{
      run_id: @read_model_run_id,
      workflow: @read_model_workflow,
      occurred_at: @read_model_started_at
    })
  end

  defp read_model_runnables_planned do
    read_model_entry!(:runnables_planned, %{
      run_id: @read_model_run_id,
      runnables: [read_model_planned_runnable()],
      occurred_at: @read_model_visible_at
    })
  end

  defp read_model_attempt_scheduled do
    read_model_entry!(:attempt_scheduled, read_model_scheduled_attrs())
  end

  defp read_model_planned_runnable do
    Map.delete(read_model_scheduled_attrs(), :occurred_at)
  end

  defp read_model_scheduled_attrs do
    %{
      run_id: @read_model_run_id,
      runnable_key: @read_model_runnable_key,
      idempotency_key: @read_model_idempotency_key,
      attempt_number: 1,
      queue: @read_model_queue,
      step: "charge_card",
      input: %{"payment_id" => "pay_123"},
      visible_at: @read_model_visible_at,
      occurred_at: @read_model_started_at
    }
  end

  defp read_model_entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp claim_token_hash(token) do
    Base.encode16(:crypto.hash(:sha256, token), case: :lower)
  end

  defp journal_start_runnable(run_id, account_id \\ "acct_123") do
    %{
      run_id: run_id,
      runnable_key: "#{run_id}:check_gateway:1",
      idempotency_key: "#{run_id}:check_gateway:1",
      attempt_number: 1,
      queue: @read_model_queue,
      step: "check_gateway",
      input: %{account_id: account_id},
      visible_at: @read_model_started_at
    }
  end

  defp journal_missing_path_runnable(run_id) do
    %{
      run_id: run_id,
      runnable_key: "#{run_id}:load_review_context:1",
      idempotency_key: "#{run_id}:load_review_context:1",
      attempt_number: 1,
      queue: @read_model_queue,
      step: "load_review_context",
      input: %{draft: %{}},
      visible_at: @read_model_started_at
    }
  end

  defp read_model_table_name(:checkpoints),
    do: :squid_mesh_read_model_squid_mesh_test_checkpoints

  defp read_model_table_name(:threads),
    do: :squid_mesh_read_model_squid_mesh_test_threads

  defp read_model_table_name(:thread_meta),
    do: :squid_mesh_read_model_squid_mesh_test_thread_meta

  defp cleanup_read_model_storage do
    for suffix <- [:checkpoints, :threads, :thread_meta] do
      delete_table_if_present(read_model_table_name(suffix))
    end
  end

  defp delete_table_if_present(table) do
    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end

  defp put_squid_mesh_config(overrides) do
    original_config = Application.get_all_env(:squid_mesh)

    on_exit(fn ->
      :squid_mesh
      |> Application.get_all_env()
      |> Keyword.keys()
      |> Enum.each(&Application.delete_env(:squid_mesh, &1))

      Enum.each(original_config, fn {key, value} ->
        Application.put_env(:squid_mesh, key, value)
      end)
    end)

    Enum.each(overrides, fn {key, value} ->
      Application.put_env(:squid_mesh, key, value)
    end)
  end
end
