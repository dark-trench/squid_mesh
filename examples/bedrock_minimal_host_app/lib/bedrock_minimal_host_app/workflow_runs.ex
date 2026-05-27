defmodule BedrockMinimalHostApp.WorkflowRuns do
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

  @type nested_invite_delivery_attrs :: %{
          required(:party_id) => String.t(),
          required(:guest_id) => String.t(),
          required(:child_queue) => String.t(),
          optional(:fail_after_child_start) => boolean(),
          optional(:fail_child_once) => boolean()
        }

  @type run_result :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  @type explanation_result :: SquidMesh.ReadModel.Explanation.Diagnostic.t()
  @type listing_result :: SquidMesh.ReadModel.Listing.Summary.t()

  alias SquidMesh.Runtime.Signal

  @spec start_payment_recovery(payment_recovery_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_payment_recovery(attrs) when is_map(attrs) do
    SquidMesh.start(BedrockMinimalHostApp.Workflows.PaymentRecovery, :payment_recovery, attrs)
  end

  @spec start_cancellable_wait(cancellable_wait_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_cancellable_wait(attrs) when is_map(attrs) do
    SquidMesh.start(BedrockMinimalHostApp.Workflows.CancellableWait, attrs)
  end

  @spec start_retry_verification(retry_verification_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_retry_verification(attrs) when is_map(attrs) do
    SquidMesh.start(
      BedrockMinimalHostApp.Workflows.RetryVerification,
      :retry_verification,
      attrs
    )
  end

  @spec start_dependency_recovery(dependency_recovery_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_dependency_recovery(attrs) when is_map(attrs) do
    SquidMesh.start(
      BedrockMinimalHostApp.Workflows.DependencyRecovery,
      :dependency_recovery,
      attrs
    )
  end

  @spec start_manual_approval(manual_approval_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_manual_approval(attrs) when is_map(attrs) do
    SquidMesh.start(BedrockMinimalHostApp.Workflows.ManualApproval, :manual_approval, attrs)
  end

  @spec start_manual_digest(manual_digest_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_manual_digest(attrs) when is_map(attrs) do
    SquidMesh.start(BedrockMinimalHostApp.Workflows.DailyDigest, :manual_digest, attrs)
  end

  @doc """
  Starts the saga checkout example that compensates completed side effects.
  """
  @spec start_saga_checkout(saga_checkout_attrs()) :: {:ok, run_result()} | {:error, term()}
  def start_saga_checkout(attrs) when is_map(attrs) do
    SquidMesh.start(BedrockMinimalHostApp.Workflows.SagaCheckout, :saga_checkout, attrs)
  end

  @doc """
  Starts the local ledger checkout example that uses one host repo transaction.
  """
  @spec start_local_ledger_checkout(local_ledger_checkout_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_local_ledger_checkout(attrs) when is_map(attrs) do
    SquidMesh.start(
      BedrockMinimalHostApp.Workflows.LocalLedgerCheckout,
      :local_ledger_checkout,
      attrs
    )
  end

  @doc """
  Starts the nested invite delivery example that creates a child workflow run.
  """
  @spec start_nested_invite_delivery(nested_invite_delivery_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_nested_invite_delivery(attrs) when is_map(attrs) do
    SquidMesh.start(
      BedrockMinimalHostApp.Workflows.NestedInviteDelivery,
      :nested_invite_delivery,
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

  @spec cancel(Ecto.UUID.t()) :: {:ok, run_result()} | {:error, term()}
  def cancel(run_id) do
    with {:ok, signal} <-
           Signal.cancel_run(run_id,
             metadata: %{source: "bedrock_minimal_host_app.workflow_runs"}
           ) do
      SquidMesh.apply_signal(signal)
    end
  end

  @spec resume(Ecto.UUID.t()) :: {:ok, run_result()} | {:error, term()}
  def resume(run_id), do: resume(run_id, %{})

  @spec resume(Ecto.UUID.t(), map()) :: {:ok, run_result()} | {:error, term()}
  def resume(run_id, attrs) when is_map(attrs) do
    with {:ok, signal} <- Signal.resume_run(run_id, attrs) do
      SquidMesh.apply_signal(signal)
    end
  end

  @spec approve(Ecto.UUID.t(), map()) :: {:ok, run_result()} | {:error, term()}
  def approve(run_id, attrs) when is_map(attrs) do
    with {:ok, signal} <- Signal.approve_run(run_id, attrs) do
      SquidMesh.apply_signal(signal)
    end
  end

  @spec reject(Ecto.UUID.t(), map()) :: {:ok, run_result()} | {:error, term()}
  def reject(run_id, attrs) when is_map(attrs) do
    with {:ok, signal} <- Signal.reject_run(run_id, attrs) do
      SquidMesh.apply_signal(signal)
    end
  end

  @spec replay(Ecto.UUID.t()) :: {:ok, run_result()} | {:error, term()}
  def replay(run_id) do
    SquidMesh.replay(run_id)
  end

  @spec list_daily_digest_runs() :: {:ok, [listing_result()]} | {:error, term()}
  def list_daily_digest_runs do
    SquidMesh.list_runs(workflow: BedrockMinimalHostApp.Workflows.DailyDigest)
  end
end
