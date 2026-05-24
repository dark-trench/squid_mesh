defmodule MinimalHostApp.WorkflowRuns do
  @moduledoc """
  Application-facing boundary for workflow operations in the example host app.

  A real Phoenix or OTP application would call Squid Mesh from a context or
  service like this one rather than directly from controllers or jobs.
  """

  @type payment_recovery_attrs :: %{
          required(:account_id) => String.t(),
          required(:invoice_id) => String.t(),
          required(:attempt_id) => String.t(),
          required(:gateway_url) => String.t()
        }

  @type cancellable_wait_attrs :: %{
          required(:account_id) => String.t()
        }

  @type retry_verification_attrs :: %{
          required(:attempt_id) => String.t()
        }

  @type dependency_recovery_attrs :: %{
          required(:account_id) => String.t(),
          required(:invoice_id) => String.t(),
          required(:attempt_id) => String.t()
        }

  @type manual_approval_attrs :: %{
          required(:account_id) => String.t()
        }

  @type manual_digest_attrs :: %{
          required(:channel) => String.t(),
          required(:digest_date) => String.t()
        }

  @type saga_checkout_attrs :: %{
          required(:account_id) => String.t(),
          required(:order_id) => String.t()
        }

  @type local_ledger_checkout_attrs :: %{
          required(:account_id) => String.t(),
          optional(:fail_after_reserve) => boolean()
        }

  @type run_result ::
          SquidMesh.ReadModel.Inspection.Snapshot.t()

  @type explanation_result ::
          SquidMesh.ReadModel.Explanation.Diagnostic.t()

  @type listing_result :: SquidMesh.ReadModel.Listing.Summary.t()

  @spec start_payment_recovery(payment_recovery_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_payment_recovery(attrs) when is_map(attrs) do
    SquidMesh.start_run(MinimalHostApp.Workflows.PaymentRecovery, :payment_recovery, attrs)
  end

  @spec start_cancellable_wait(cancellable_wait_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_cancellable_wait(attrs) when is_map(attrs) do
    SquidMesh.start_run(MinimalHostApp.Workflows.CancellableWait, attrs)
  end

  @spec start_retry_verification(retry_verification_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_retry_verification(attrs) when is_map(attrs) do
    SquidMesh.start_run(MinimalHostApp.Workflows.RetryVerification, :retry_verification, attrs)
  end

  @spec start_dependency_recovery(dependency_recovery_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_dependency_recovery(attrs) when is_map(attrs) do
    SquidMesh.start_run(MinimalHostApp.Workflows.DependencyRecovery, :dependency_recovery, attrs)
  end

  @spec start_manual_approval(manual_approval_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_manual_approval(attrs) when is_map(attrs) do
    SquidMesh.start_run(MinimalHostApp.Workflows.ManualApproval, :manual_approval, attrs)
  end

  @spec start_manual_digest(manual_digest_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_manual_digest(attrs) when is_map(attrs) do
    SquidMesh.start_run(MinimalHostApp.Workflows.DailyDigest, :manual_digest, attrs)
  end

  @doc """
  Starts the saga checkout example that compensates completed side effects.
  """
  @spec start_saga_checkout(saga_checkout_attrs()) :: {:ok, run_result()} | {:error, term()}
  def start_saga_checkout(attrs) when is_map(attrs) do
    SquidMesh.start_run(MinimalHostApp.Workflows.SagaCheckout, :saga_checkout, attrs)
  end

  @doc """
  Starts the local ledger checkout example that uses one host repo transaction.
  """
  @spec start_local_ledger_checkout(local_ledger_checkout_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_local_ledger_checkout(attrs) when is_map(attrs) do
    SquidMesh.start_run(
      MinimalHostApp.Workflows.LocalLedgerCheckout,
      :local_ledger_checkout,
      attrs
    )
  end

  @spec inspect_payment_recovery(Ecto.UUID.t()) :: {:ok, run_result()} | {:error, term()}
  def inspect_payment_recovery(run_id) do
    SquidMesh.inspect_run(run_id)
  end

  @spec inspect_run(Ecto.UUID.t(), keyword()) :: {:ok, run_result()} | {:error, term()}
  def inspect_run(run_id, opts \\ []) do
    SquidMesh.inspect_run(run_id, opts)
  end

  @spec explain_run(Ecto.UUID.t()) :: {:ok, explanation_result()} | {:error, term()}
  def explain_run(run_id) do
    SquidMesh.explain_run(run_id)
  end

  @spec cancel_run(Ecto.UUID.t()) :: {:ok, run_result()} | {:error, term()}
  def cancel_run(run_id) do
    SquidMesh.cancel_run(run_id)
  end

  @spec unblock_run(Ecto.UUID.t()) :: {:ok, run_result()} | {:error, term()}
  def unblock_run(run_id), do: SquidMesh.unblock_run(run_id)

  @spec unblock_run(Ecto.UUID.t(), map()) :: {:ok, run_result()} | {:error, term()}
  def unblock_run(run_id, attrs) when is_map(attrs) do
    SquidMesh.unblock_run(run_id, attrs)
  end

  @spec approve_run(Ecto.UUID.t(), map()) :: {:ok, run_result()} | {:error, term()}
  def approve_run(run_id, attrs) when is_map(attrs) do
    SquidMesh.approve_run(run_id, attrs)
  end

  @spec reject_run(Ecto.UUID.t(), map()) :: {:ok, run_result()} | {:error, term()}
  def reject_run(run_id, attrs) when is_map(attrs) do
    SquidMesh.reject_run(run_id, attrs)
  end

  @spec replay_run(Ecto.UUID.t()) :: {:ok, run_result()} | {:error, term()}
  def replay_run(run_id) do
    SquidMesh.replay_run(run_id)
  end

  @spec list_dependency_recovery_runs(keyword()) :: {:ok, [listing_result()]} | {:error, term()}
  def list_dependency_recovery_runs(opts \\ []) do
    SquidMesh.list_runs([workflow: MinimalHostApp.Workflows.DependencyRecovery], opts)
  end

  @spec list_runs(keyword()) :: {:ok, [listing_result()]} | {:error, term()}
  def list_runs(opts \\ []) do
    SquidMesh.list_runs([], opts)
  end

  @spec list_daily_digest_runs() :: {:ok, [listing_result()]} | {:error, term()}
  def list_daily_digest_runs do
    SquidMesh.list_runs(workflow: MinimalHostApp.Workflows.DailyDigest)
  end
end
