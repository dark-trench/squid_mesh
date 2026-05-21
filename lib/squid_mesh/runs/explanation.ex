defmodule SquidMesh.Runs.Explanation do
  @moduledoc """
  Structured diagnostic explanation for one workflow run.

  Explanations are transport-friendly data. They point back to persisted runtime
  facts so host apps can render their own operator messages without scraping
  logs or interpreting telemetry.
  """

  import Ecto.Query

  alias SquidMesh.Config
  alias SquidMesh.Run
  alias SquidMesh.Runs
  alias SquidMesh.Runs.StepState
  alias SquidMesh.Runtime.RetryPolicy
  alias SquidMesh.Steps.Attempt
  alias SquidMesh.Steps.Execution

  @type reason ::
          :pending_dispatch
          | :step_scheduled
          | :step_running
          | :waiting_for_dependencies
          | :waiting_for_retry
          | :retry_exhausted
          | :step_failed
          | :paused_for_manual_action
          | :paused_for_approval
          | :paused_with_missing_resume_metadata
          | :paused_with_invalid_resume_target
          | :paused_with_unavailable_workflow
          | :cancelling
          | :cancelled
          | :completed

  @type next_action ::
          :wait_for_step
          | :wait_for_dependencies
          | :wait_for_retry
          | :wait_for_cancellation
          | :unblock_run
          | :approve_run
          | :reject_run
          | :cancel_run
          | :replay_run

  @type t :: %__MODULE__{
          status: Run.status(),
          reason: reason(),
          step: atom() | String.t() | nil,
          details: map(),
          next_actions: [next_action()],
          evidence: map()
        }

  defstruct [
    :status,
    :reason,
    :step,
    details: %{},
    next_actions: [],
    evidence: %{}
  ]

  @doc false
  @spec explain(Config.t(), Ecto.UUID.t()) :: {:ok, t()} | {:error, Runs.Store.get_error()}
  def explain(%Config{} = config, run_id) do
    with {:ok, run} <- Runs.Store.get_run(config.repo, run_id, include_history: true) do
      {:ok, build(config, run, workflow_definition(run))}
    end
  end

  defp build(%Config{} = _config, %Run{status: :completed} = run, definition) do
    terminal_explanation(run, definition, :completed)
  end

  defp build(%Config{} = _config, %Run{status: :cancelled} = run, definition) do
    terminal_explanation(run, definition, :cancelled)
  end

  defp build(%Config{} = _config, %Run{status: :cancelling} = run, _definition) do
    explanation(
      run,
      :cancelling,
      run.current_step,
      %{},
      [:wait_for_cancellation],
      base_evidence(run)
    )
  end

  defp build(%Config{} = config, %Run{status: :paused} = run, definition) do
    explain_paused(config, run, definition)
  end

  defp build(%Config{} = _config, %Run{status: :retrying} = run, _definition) do
    {step_run, attempt} = current_step_attempt(run)

    details =
      compact(%{
        next_attempt_number: next_attempt_number(attempt),
        last_error: run.last_error
      })

    evidence =
      run
      |> base_evidence()
      |> Map.merge(step_evidence(step_run, attempt))

    explanation(
      run,
      :waiting_for_retry,
      run.current_step,
      details,
      [:wait_for_retry, :cancel_run],
      evidence
    )
  end

  defp build(%Config{} = _config, %Run{status: :failed} = run, definition) do
    {step_run, attempt} = current_step_attempt(run)
    {reason, retry_details} = failure_reason(run, step_run, attempt)

    details =
      %{
        last_error: run.last_error
      }
      |> Map.merge(retry_details)
      |> compact()

    evidence =
      run
      |> evidence_with_workflow_definition(definition)
      |> Map.merge(step_evidence(step_run, attempt))

    final_details = Map.merge(details, terminal_details(run, definition))

    explanation(
      run,
      reason,
      run.current_step,
      final_details,
      replay_actions(run, definition),
      evidence
    )
  end

  defp build(%Config{} = config, %Run{} = run, _definition) do
    cond do
      dependency_wait?(run) ->
        explain_dependency_wait(run)

      dependency_join_scheduled?(run) ->
        explain_dependency_join_scheduled(run)

      current_step_running?(run) ->
        explain_running_step(config, run)

      run.status == :pending ->
        explanation(
          run,
          :pending_dispatch,
          run.current_step,
          %{},
          [:wait_for_step, :cancel_run],
          base_evidence(run)
        )

      true ->
        explanation(
          run,
          :step_scheduled,
          run.current_step,
          %{},
          [:wait_for_step, :cancel_run],
          base_evidence(run)
        )
    end
  end

  defp explain_paused(%Config{} = config, %Run{} = run, definition) do
    persisted_step_run = persisted_current_step_run(config.repo, run)
    resume = persisted_step_run && persisted_step_run.resume
    {step_run, attempt} = current_step_attempt(run)

    case {definition, deserialize_resume(resume, definition)} do
      {nil, {:ok, resume_details}} ->
        unavailable_workflow_explanation(run, step_run, attempt, resume_details)

      {_definition, {:ok, %{kind: :approval} = resume_details}} ->
        if invalid_approval_targets?(definition, resume_details) do
          invalid_resume_explanation(run, step_run, attempt, resume_details)
        else
          details = %{approval_targets: Map.take(resume_details, [:ok, :error])}

          evidence =
            run
            |> base_evidence()
            |> Map.merge(step_evidence(step_run, attempt))
            |> Map.put(
              :step_run,
              Map.merge(step_run_evidence(step_run), %{resume: resume_details})
            )

          explanation(
            run,
            :paused_for_approval,
            run.current_step,
            details,
            [:approve_run, :reject_run, :cancel_run],
            evidence
          )
        end

      {_definition, {:ok, %{kind: :pause, target: target} = resume_details}} ->
        if invalid_resume_target?(definition, target) do
          invalid_resume_explanation(run, step_run, attempt, resume_details)
        else
          evidence =
            run
            |> base_evidence()
            |> Map.merge(step_evidence(step_run, attempt))
            |> Map.put(
              :step_run,
              Map.merge(step_run_evidence(step_run), %{resume: resume_details})
            )

          explanation(
            run,
            :paused_for_manual_action,
            run.current_step,
            %{resume_target: target},
            [:unblock_run, :cancel_run],
            evidence
          )
        end

      {_definition, {:missing, details}} ->
        evidence =
          run
          |> base_evidence()
          |> Map.merge(step_evidence(step_run, attempt))

        explanation(
          run,
          :paused_with_missing_resume_metadata,
          run.current_step,
          details,
          [:cancel_run],
          evidence
        )
    end
  end

  defp terminal_explanation(run, definition, reason) do
    explanation(
      run,
      reason,
      nil,
      terminal_details(run, definition),
      replay_actions(run, definition),
      evidence_with_workflow_definition(run, definition)
    )
  end

  defp unavailable_workflow_explanation(run, step_run, attempt, resume_details) do
    evidence =
      run
      |> evidence_with_workflow_definition(nil)
      |> Map.merge(step_evidence(step_run, attempt))
      |> Map.put(:step_run, Map.merge(step_run_evidence(step_run), %{resume: resume_details}))

    explanation(
      run,
      :paused_with_unavailable_workflow,
      run.current_step,
      %{workflow_definition: :unavailable},
      [:cancel_run],
      evidence
    )
  end

  defp invalid_resume_explanation(run, step_run, attempt, resume_details) do
    evidence =
      run
      |> base_evidence()
      |> Map.merge(step_evidence(step_run, attempt))
      |> Map.put(:step_run, Map.merge(step_run_evidence(step_run), %{resume: resume_details}))

    explanation(
      run,
      :paused_with_invalid_resume_target,
      run.current_step,
      invalid_resume_details(resume_details),
      [:cancel_run],
      evidence
    )
  end

  defp explain_dependency_wait(%Run{} = run) do
    waiting_step =
      Enum.find(run.steps, fn step ->
        step.status == :waiting and Enum.any?(step.depends_on, &dependency_incomplete?(run, &1))
      end)

    waiting_on = Enum.filter(waiting_step.depends_on, &dependency_incomplete?(run, &1))

    details = %{
      waiting_on: waiting_on,
      dependency_statuses: dependency_statuses(run, waiting_step.depends_on)
    }

    evidence = Map.put(base_evidence(run), :steps, Enum.map(run.steps, &step_state_evidence/1))

    explanation(
      run,
      :waiting_for_dependencies,
      waiting_step.step,
      details,
      [:wait_for_dependencies, :cancel_run],
      evidence
    )
  end

  defp explain_dependency_join_scheduled(%Run{} = run) do
    scheduled_step = scheduled_dependency_join(run)

    details = %{satisfied_dependencies: scheduled_step.depends_on}
    evidence = Map.put(base_evidence(run), :steps, Enum.map(run.steps, &step_state_evidence/1))

    explanation(
      run,
      :step_scheduled,
      scheduled_step.step,
      details,
      [:wait_for_step, :cancel_run],
      evidence
    )
  end

  defp explain_running_step(%Config{} = config, %Run{} = run) do
    {step_run, attempt} = current_step_attempt(run)

    details =
      compact(%{
        duplicate_delivery_policy: :skip_while_running,
        stale_step_reclaim: stale_step_reclaim(config)
      })

    evidence =
      run
      |> base_evidence()
      |> Map.merge(step_evidence(step_run, attempt))

    explanation(
      run,
      :step_running,
      run.current_step,
      details,
      [:wait_for_step, :cancel_run],
      evidence
    )
  end

  defp dependency_wait?(%Run{steps: steps}) when is_list(steps) do
    Enum.any?(steps, fn step ->
      step.status == :waiting and Enum.any?(step.depends_on, &dependency_incomplete?(steps, &1))
    end)
  end

  defp dependency_wait?(_run), do: false

  defp dependency_join_scheduled?(%Run{} = run), do: not is_nil(scheduled_dependency_join(run))

  defp scheduled_dependency_join(%Run{steps: steps}) when is_list(steps) do
    Enum.find(steps, fn
      %StepState{status: :pending, depends_on: dependencies} when dependencies != [] ->
        Enum.all?(dependencies, &(not dependency_incomplete?(steps, &1)))

      _step ->
        false
    end)
  end

  defp scheduled_dependency_join(_run), do: nil

  defp dependency_incomplete?(%Run{steps: steps}, dependency),
    do: dependency_incomplete?(steps, dependency)

  defp dependency_incomplete?(steps, dependency) when is_list(steps) do
    case Enum.find(steps, &(&1.step == dependency)) do
      %StepState{status: :completed} -> false
      %StepState{} -> true
      nil -> true
    end
  end

  defp dependency_statuses(%Run{steps: steps}, dependencies) when is_list(dependencies) do
    Map.new(dependencies, fn dependency ->
      {dependency, dependency_status(steps, dependency)}
    end)
  end

  defp dependency_status(steps, dependency) when is_list(steps) do
    case Enum.find(steps, &(&1.step == dependency)) do
      %StepState{status: status} -> status
      nil -> :missing
    end
  end

  defp current_step_running?(run) do
    case current_step_run(run) do
      %Execution{status: :running} -> true
      _other -> false
    end
  end

  defp current_step_attempt(%Run{} = run) do
    step_run = current_step_run(run)
    {step_run, latest_attempt(step_run)}
  end

  defp current_step_run(%Run{step_runs: step_runs, current_step: step}) when is_list(step_runs) do
    Enum.find(step_runs, &(&1.step == step))
  end

  defp current_step_run(_run), do: nil

  defp latest_attempt(%Execution{attempts: attempts}) when is_list(attempts) do
    Enum.max_by(attempts, & &1.attempt_number, fn -> nil end)
  end

  defp latest_attempt(_step_run), do: nil

  defp next_attempt_number(%Attempt{attempt_number: attempt_number}), do: attempt_number + 1
  defp next_attempt_number(_attempt), do: nil

  defp failure_reason(%Run{workflow: workflow, current_step: step}, _step_run, attempt)
       when is_atom(workflow) and is_atom(step) do
    case {RetryPolicy.max_attempts(workflow, step), attempt} do
      {{:ok, max_attempts}, %Attempt{attempt_number: attempt_number}}
      when attempt_number >= max_attempts ->
        {:retry_exhausted, %{max_attempts: max_attempts, latest_attempt_number: attempt_number}}

      _other ->
        {:step_failed, %{}}
    end
  end

  defp failure_reason(_run, _step_run, _attempt), do: {:step_failed, %{}}

  defp persisted_current_step_run(_repo, %Run{current_step: nil}), do: nil

  defp persisted_current_step_run(repo, %Run{id: run_id, current_step: step}) do
    serialized_step = serialize_step(step)

    SquidMesh.Persistence.StepRun
    |> where([step_run], step_run.run_id == ^run_id and step_run.step == ^serialized_step)
    |> order_by([step_run], desc: step_run.updated_at, desc: step_run.id)
    |> limit(1)
    |> repo.one()
  end

  defp deserialize_resume(%{"kind" => "approval"} = resume, definition) do
    {:ok,
     %{
       kind: :approval,
       ok: deserialize_target(definition, Map.get(resume, "ok_target")),
       error: deserialize_target(definition, Map.get(resume, "error_target")),
       output_key: Map.get(resume, "output_key")
     }}
  end

  defp deserialize_resume(%{"target" => target} = resume, definition) do
    {:ok,
     %{
       kind: :pause,
       target: deserialize_target(definition, target),
       output: Map.get(resume, "output")
     }}
  end

  defp deserialize_resume(nil, _definition), do: {:missing, %{resume: :missing}}
  defp deserialize_resume(_resume, _definition), do: {:missing, %{resume: :invalid_shape}}

  defp deserialize_target(_definition, "__complete__"), do: :complete
  defp deserialize_target(nil, target), do: target

  defp deserialize_target(definition, target) when is_binary(target) do
    SquidMesh.Workflow.Definition.deserialize_step(definition, target)
  end

  defp invalid_resume_target?(_definition, :complete), do: false
  defp invalid_resume_target?(nil, _target), do: false
  defp invalid_resume_target?(_definition, target) when is_atom(target), do: false
  defp invalid_resume_target?(_definition, _target), do: true

  defp invalid_approval_targets?(definition, resume_details) do
    invalid_resume_target?(definition, resume_details.ok) or
      invalid_resume_target?(definition, resume_details.error)
  end

  defp invalid_resume_details(%{kind: :approval} = resume_details) do
    %{approval_targets: Map.take(resume_details, [:ok, :error])}
  end

  defp invalid_resume_details(%{target: target}), do: %{resume_target: target}

  defp workflow_definition(%Run{workflow: workflow}) when is_atom(workflow) do
    case SquidMesh.Workflow.Definition.load(workflow) do
      {:ok, definition} -> definition
      {:error, _reason} -> nil
    end
  end

  defp workflow_definition(_run), do: nil

  defp stale_step_reclaim(%Config{stale_step_timeout: :disabled}) do
    %{enabled: false}
  end

  defp stale_step_reclaim(%Config{stale_step_timeout: timeout_ms}) do
    %{enabled: true, timeout_ms: timeout_ms}
  end

  defp explanation(run, reason, step, details, next_actions, evidence) do
    %__MODULE__{
      status: run.status,
      reason: reason,
      step: step,
      details: details,
      next_actions: next_actions,
      evidence: evidence
    }
  end

  defp base_evidence(%Run{} = run) do
    run_evidence =
      maybe_put(
        %{
          id: run.id,
          status: run.status,
          workflow: run.workflow,
          current_step: run.current_step,
          last_error: run.last_error,
          inserted_at: run.inserted_at,
          updated_at: run.updated_at
        },
        :schedule,
        schedule_context(run.context || %{})
      )

    %{
      run: run_evidence
    }
  end

  defp schedule_context(context) do
    case Map.fetch(context, :schedule) do
      {:ok, schedule} -> schedule
      :error -> Map.get(context, "schedule")
    end
  end

  defp evidence_with_workflow_definition(%Run{} = run, nil) do
    run
    |> base_evidence()
    |> Map.put(:workflow_definition, %{available?: false})
  end

  defp evidence_with_workflow_definition(%Run{} = run, _definition) do
    base_evidence(run)
  end

  defp workflow_definition_details(nil), do: %{workflow_definition: :unavailable}
  defp workflow_definition_details(_definition), do: %{}

  defp terminal_details(run, definition) do
    definition
    |> workflow_definition_details()
    |> Map.merge(replay_details(run, definition))
  end

  defp replay_details(_run, nil), do: %{}

  defp replay_details(run, definition) do
    case unsafe_replay_steps(run, definition) do
      [] ->
        %{replay: %{allowed?: true}}

      steps ->
        %{
          replay: %{
            allowed?: false,
            required_override: :allow_irreversible,
            blocked_by: steps
          }
        }
    end
  end

  defp replay_actions(_run, nil), do: []

  defp replay_actions(run, definition) do
    case unsafe_replay_steps(run, definition) do
      [] -> [:replay_run]
      _steps -> []
    end
  end

  defp unsafe_replay_steps(%Run{step_runs: step_runs}, definition) when is_list(step_runs) do
    completed_steps =
      step_runs
      |> Enum.filter(&(&1.status == :completed))
      |> Enum.map(&{&1.step, &1.recovery})

    SquidMesh.Workflow.Definition.unsafe_replay_steps(definition, completed_steps)
  end

  defp unsafe_replay_steps(_run, _definition), do: []

  defp step_evidence(nil, nil), do: %{}

  defp step_evidence(step_run, attempt) do
    %{}
    |> maybe_put(:step_run, step_run_evidence(step_run))
    |> maybe_put(:attempt, attempt_evidence(attempt))
  end

  defp step_run_evidence(nil), do: nil

  defp step_run_evidence(%Execution{} = step_run) do
    %{
      id: step_run.id,
      step: step_run.step,
      status: step_run.status,
      last_error: step_run.last_error,
      inserted_at: step_run.inserted_at,
      updated_at: step_run.updated_at
    }
  end

  defp attempt_evidence(nil), do: nil

  defp attempt_evidence(%Attempt{} = attempt) do
    %{
      id: attempt.id,
      attempt_number: attempt.attempt_number,
      status: attempt.status,
      error: attempt.error,
      inserted_at: attempt.inserted_at,
      updated_at: attempt.updated_at
    }
  end

  defp step_state_evidence(%StepState{} = step) do
    %{
      step: step.step,
      status: step.status,
      depends_on: step.depends_on,
      last_error: step.last_error
    }
  end

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp serialize_step(step) when is_atom(step), do: Atom.to_string(step)
  defp serialize_step(step) when is_binary(step), do: step
end
