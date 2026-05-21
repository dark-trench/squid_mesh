defmodule SquidMesh.JournalProjection.Inspection.Snapshot do
  @moduledoc """
  Projection-backed inspection snapshot for one Jido-native workflow run.

  This struct is a compact read model built from the workflow and dispatch
  journal projections. It is intentionally separate from the current
  table-backed `SquidMesh.Run` struct so the Jido-native runtime can grow its
  inspection surface without changing the stable public API prematurely.

  Terminal runs keep both `terminal?` and `terminal_status` so operator-facing
  surfaces can suppress recovery actions while still distinguishing completed,
  failed, and cancelled histories.

  Future-visible attempts are kept separate from currently visible attempts.
  This lets operator-facing surfaces explain delayed retry or deferred dispatch
  state without treating the run as idle or recoverable.
  """

  @type reason ::
          :terminal
          | :completed_result_pending_apply
          | :planned_dispatch_pending_schedule
          | :expired_claim
          | :attempt_claimed
          | :attempt_visible
          | :attempt_scheduled_for_later
          | :manual_intervention_required
          | :run_started
          | :idle
          | :waiting_for_dispatch

  @type attempt :: %{
          required(:runnable_key) => String.t(),
          required(:status) => atom(),
          required(:attempt_number) => pos_integer(),
          required(:step) => String.t(),
          required(:input) => map(),
          required(:visible_at) => DateTime.t(),
          required(:idempotency_key) => String.t(),
          optional(:claim_id) => String.t(),
          optional(:owner_id) => String.t(),
          optional(:lease_until) => DateTime.t(),
          optional(:result) => map(),
          optional(:error) => map(),
          required(:wakeup_emitted?) => boolean(),
          required(:applied?) => boolean()
        }

  @type t :: %__MODULE__{
          run_id: String.t(),
          workflow: String.t() | nil,
          queue: String.t(),
          status: atom(),
          reason: reason(),
          terminal?: boolean(),
          terminal_status: atom() | nil,
          manual_state: map() | nil,
          thread_revisions: %{run: non_neg_integer(), dispatch: non_neg_integer()},
          planned_runnables: [map()],
          planned_runnable_keys: [String.t()],
          applied_runnable_keys: [String.t()],
          pending_dispatches: [map()],
          pending_results: [attempt()],
          visible_attempts: [attempt()],
          scheduled_attempts: [attempt()],
          next_visible_at: DateTime.t() | nil,
          expired_claims: [attempt()],
          attempts: [attempt()],
          anomalies: [map()]
        }

  @enforce_keys [
    :run_id,
    :workflow,
    :queue,
    :status,
    :reason,
    :terminal?,
    :terminal_status,
    :thread_revisions
  ]

  defstruct [
    :run_id,
    :workflow,
    :queue,
    :status,
    :reason,
    :terminal?,
    :terminal_status,
    :thread_revisions,
    manual_state: nil,
    planned_runnables: [],
    planned_runnable_keys: [],
    applied_runnable_keys: [],
    pending_dispatches: [],
    pending_results: [],
    visible_attempts: [],
    scheduled_attempts: [],
    next_visible_at: nil,
    expired_claims: [],
    attempts: [],
    anomalies: []
  ]
end
