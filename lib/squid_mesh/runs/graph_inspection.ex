defmodule SquidMesh.Runs.GraphInspection do
  @moduledoc """
  Graph-oriented inspection output for one workflow run.

  This projection is read-only and executor-agnostic. It is built from the same
  durable table or journal inspection data returned by `SquidMesh.inspect_run/2`,
  then overlays the declared workflow graph when the workflow module can still
  be loaded.
  """

  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.Run
  alias SquidMesh.Runs.GraphInspection.Edge
  alias SquidMesh.Runs.GraphInspection.Node
  alias SquidMesh.Workflow.Definition

  @terminal_statuses [:completed, :failed, :cancelled]

  @type source :: :runtime_tables | :read_model

  @type t :: %__MODULE__{
          run_id: String.t(),
          workflow: module() | String.t() | nil,
          source: source(),
          status: atom(),
          current_node_id: String.t() | nil,
          current_node_ids: [String.t()],
          terminal?: boolean(),
          nodes: [Node.t()],
          edges: [Edge.t()],
          anomalies: [map()]
        }

  @enforce_keys [:run_id, :workflow, :source, :status, :terminal?]

  defstruct [
    :run_id,
    :workflow,
    :source,
    :status,
    :current_node_id,
    :terminal?,
    current_node_ids: [],
    nodes: [],
    edges: [],
    anomalies: []
  ]

  @doc false
  @spec from_run(Run.t(), keyword()) :: t()
  def from_run(%Run{} = run, opts) when is_list(opts) do
    source = Keyword.get(opts, :source, :runtime_tables)
    include_details? = Keyword.get(opts, :include_details, false)
    definition = load_definition(run.workflow)
    fallback_current_node_id = current_node_id(run.current_step, run.status)
    initial_nodes = runtime_nodes(run, definition, include_details?)
    current_node_ids = runtime_current_node_ids(run, initial_nodes, fallback_current_node_id)
    current_node_id = List.first(current_node_ids)
    nodes = mark_current_nodes(initial_nodes, current_node_ids)

    %__MODULE__{
      run_id: run.id,
      workflow: run.workflow,
      source: source,
      status: run.status,
      current_node_id: current_node_id,
      current_node_ids: current_node_ids,
      terminal?: terminal_status?(run.status),
      nodes: nodes,
      edges: graph_edges(definition, nodes),
      anomalies: []
    }
  end

  @doc false
  @spec from_snapshot(Snapshot.t(), keyword()) :: t()
  def from_snapshot(%Snapshot{} = snapshot, opts) when is_list(opts) do
    source = Keyword.get(opts, :source, :read_model)
    include_details? = Keyword.get(opts, :include_details, false)
    definition = load_definition(snapshot.workflow)
    current_node_ids = snapshot_current_node_ids(snapshot)
    current_node_id = List.first(current_node_ids)
    initial_nodes = snapshot_nodes(snapshot, definition, include_details?)
    nodes = mark_current_nodes(initial_nodes, current_node_ids)

    %__MODULE__{
      run_id: snapshot.run_id,
      workflow: snapshot.workflow,
      source: source,
      status: snapshot.status,
      current_node_id: current_node_id,
      current_node_ids: current_node_ids,
      terminal?: snapshot.terminal?,
      nodes: nodes,
      edges: graph_edges(definition, nodes),
      anomalies: sanitize_anomalies(snapshot.anomalies)
    }
  end

  defp runtime_nodes(%Run{} = run, definition, include_details?) do
    step_states = run.steps || run.step_runs || []
    node_ids = ordered_node_ids(definition, step_states, &runtime_step_id/1)
    step_states_by_id = Map.new(step_states, &{runtime_step_id(&1), &1})

    Enum.map(node_ids, fn node_id ->
      step_state = Map.get(step_states_by_id, node_id)
      runtime_node(run, step_state, node_id, include_details?)
    end)
  end

  defp runtime_node(%Run{} = run, nil, node_id, include_details?) do
    %Node{
      id: node_id,
      status: :waiting,
      current?: false,
      manual_state: detail(include_details?, runtime_manual_state(run, node_id))
    }
  end

  defp runtime_node(%Run{} = run, step_state, node_id, include_details?) do
    %Node{
      id: node_id,
      status: runtime_node_status(run, step_state, node_id),
      current?: false,
      input: detail(include_details?, step_state.input),
      output: detail(include_details?, step_state.output),
      error: detail(include_details?, step_state.last_error),
      recovery: detail(include_details?, step_state.recovery),
      transition: step_state.transition,
      manual_state: detail(include_details?, runtime_manual_state(run, node_id)),
      attempts:
        detail(include_details?, Enum.map(step_state.attempts || [], &runtime_attempt/1), [])
    }
  end

  defp runtime_node_status(%Run{} = run, step_state, node_id) do
    cond do
      runtime_manual_node?(run, node_id) -> :paused
      runtime_retry_node?(run, step_state, node_id) -> :retrying
      true -> step_state.status
    end
  end

  defp runtime_manual_state(%Run{} = run, node_id) do
    if runtime_manual_node?(run, node_id), do: %{status: :paused, step: node_id}, else: nil
  end

  defp runtime_manual_node?(%Run{} = run, node_id) do
    run.status == :paused and current_node_id(run.current_step, run.status) == node_id
  end

  defp runtime_retry_node?(%Run{} = run, step_state, node_id) do
    run.status == :retrying and step_state.status == :failed and
      current_node_id(run.current_step, run.status) == node_id
  end

  defp runtime_attempt(attempt) do
    compact(%{
      attempt_number: attempt.attempt_number,
      status: attempt.status,
      error: attempt.error
    })
  end

  defp snapshot_nodes(%Snapshot{} = snapshot, definition, include_details?) do
    attempts_by_step = Enum.group_by(snapshot.attempts, &Map.fetch!(&1, :step))

    step_sources =
      snapshot.attempts ++ snapshot.planned_runnables ++ snapshot.pending_dispatches

    node_ids = ordered_node_ids(definition, step_sources, &snapshot_step_id/1)

    Enum.map(node_ids, fn node_id ->
      attempts = Map.get(attempts_by_step, node_id, [])

      %Node{
        id: node_id,
        status: snapshot_node_status(snapshot, node_id, attempts),
        current?: false,
        input: detail(include_details?, latest_attempt_value(attempts, :input)),
        output: detail(include_details?, latest_attempt_value(attempts, :result)),
        error: detail(include_details?, latest_attempt_value(attempts, :error)),
        transition:
          Definition.deserialize_transition_decision(
            definition,
            latest_attempt_value(attempts, :transition)
          ),
        manual_state: detail(include_details?, snapshot_manual_state(snapshot, node_id)),
        attempts: detail(include_details?, Enum.map(attempts, &snapshot_attempt/1), [])
      }
    end)
  end

  defp snapshot_node_status(%Snapshot{} = snapshot, node_id, attempts) do
    cond do
      manual_node?(snapshot, node_id) ->
        :paused

      Enum.any?(attempts, &(Map.get(&1, :status) == :completed and Map.get(&1, :applied?))) ->
        :completed

      Enum.any?(attempts, &(Map.get(&1, :status) == :claimed)) ->
        :running

      Enum.any?(attempts, &(Map.get(&1, :status) == :retry_scheduled)) ->
        :retrying

      Enum.any?(attempts, &(Map.get(&1, :status) in [:available, :retry_scheduled])) ->
        :pending

      Enum.any?(attempts, &(Map.get(&1, :status) == :failed)) ->
        :failed

      true ->
        :waiting
    end
  end

  defp snapshot_attempt(attempt) do
    compact(%{
      attempt_number: Map.fetch!(attempt, :attempt_number),
      status: Map.fetch!(attempt, :status),
      error: Map.get(attempt, :error)
    })
  end

  defp latest_attempt_value(attempts, key) do
    attempts
    |> Enum.reverse()
    |> Enum.find_value(&Map.get(&1, key))
  end

  defp snapshot_manual_state(%Snapshot{} = snapshot, node_id) do
    if manual_node?(snapshot, node_id), do: snapshot.manual_state, else: nil
  end

  defp manual_node?(%Snapshot{manual_state: %{step: step}}, node_id), do: step == node_id
  defp manual_node?(%Snapshot{}, _node_id), do: false

  defp snapshot_current_node_ids(%Snapshot{terminal?: true}), do: []

  defp snapshot_current_node_ids(%Snapshot{manual_state: %{step: step}}), do: [normalize_id(step)]

  defp snapshot_current_node_ids(%Snapshot{} = snapshot) do
    snapshot.visible_attempts
    |> Kernel.++(snapshot.expired_claims)
    |> Kernel.++(claimed_attempts(snapshot.attempts))
    |> Kernel.++(snapshot.scheduled_attempts)
    |> Enum.map(&normalize_id(Map.get(&1, :step)))
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp graph_edges(nil, _nodes), do: []

  defp graph_edges(definition, nodes) do
    if Definition.dependency_mode?(definition) do
      dependency_edges(definition, nodes)
    else
      transition_edges(definition, nodes)
    end
  end

  defp transition_edges(definition, nodes) do
    nodes_by_id = Map.new(nodes, &{&1.id, &1})

    definition.transitions
    |> Enum.with_index()
    |> Enum.map(fn {transition, index} ->
      from = normalize_id(transition.from)
      to = normalize_id(transition.to)
      outcome = transition.on
      from_node = Map.get(nodes_by_id, from)
      conditional_group? = conditional_transition_group?(definition.transitions, transition)

      %Edge{
        id: transition_edge_id(from, outcome, to, transition, index),
        from: from,
        to: to,
        type: :transition,
        outcome: outcome,
        condition: Map.get(transition, :condition),
        recovery: Map.get(transition, :recovery),
        status: transition_edge_status(from_node, transition, conditional_group?)
      }
    end)
  end

  defp transition_edge_id(from, outcome, to, transition, index) do
    [from, outcome, to, transition_condition_id(Map.get(transition, :condition), index)]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(":")
  end

  defp transition_condition_id(nil, _index), do: nil

  defp transition_condition_id(_condition, index) do
    "condition:#{index}"
  end

  defp conditional_transition_group?(transitions, transition) do
    Enum.any?(
      transitions,
      &(&1.from == transition.from and &1.on == transition.on and Map.has_key?(&1, :condition))
    )
  end

  defp transition_edge_status(nil, _transition, _conditional_group?), do: :pending

  defp transition_edge_status(%Node{transition: %{} = selected}, transition, _conditional_group?) do
    if selected_transition?(selected, transition), do: :selected, else: :skipped
  end

  defp transition_edge_status(%Node{status: :completed}, %{on: :ok}, true), do: :pending
  defp transition_edge_status(%Node{status: :failed}, %{on: :error}, true), do: :pending

  defp transition_edge_status(%Node{status: :completed}, %{on: :ok}, false), do: :selected
  defp transition_edge_status(%Node{status: :failed}, %{on: :error}, false), do: :selected

  defp transition_edge_status(%Node{status: status}, _transition, _conditional_group?)
       when status in [:completed, :failed], do: :skipped

  defp transition_edge_status(%Node{}, _transition, _conditional_group?), do: :pending

  defp selected_transition?(selected, transition) do
    normalize_id(Map.get(selected, :from)) == normalize_id(transition.from) and
      normalize_outcome(Map.get(selected, :on)) == transition.on and
      normalize_id(Map.get(selected, :to)) == normalize_id(transition.to) and
      normalize_condition(Map.get(selected, :condition)) ==
        normalize_condition(Map.get(transition, :condition))
  end

  defp normalize_outcome(outcome) when is_atom(outcome), do: outcome
  defp normalize_outcome("ok"), do: :ok
  defp normalize_outcome("error"), do: :error
  defp normalize_outcome(outcome), do: outcome

  defp normalize_condition(nil), do: nil

  defp normalize_condition(condition),
    do: SquidMesh.Workflow.TransitionCondition.serialize(condition)

  defp dependency_edges(definition, nodes) do
    nodes_by_id = Map.new(nodes, &{&1.id, &1})

    definition
    |> Definition.inspect_steps()
    |> Enum.flat_map(fn %{step: step, depends_on: dependencies} ->
      to = normalize_id(step)

      Enum.map(dependencies, fn dependency ->
        from = normalize_id(dependency)

        %Edge{
          id: Enum.join([from, "dependency", to], ":"),
          from: from,
          to: to,
          type: :dependency,
          status: dependency_edge_status(Map.get(nodes_by_id, from))
        }
      end)
    end)
  end

  defp dependency_edge_status(nil), do: :pending
  defp dependency_edge_status(%Node{status: :completed}), do: :selected
  defp dependency_edge_status(%Node{status: :failed}), do: :blocked
  defp dependency_edge_status(%Node{}), do: :pending

  defp claimed_attempts(attempts) do
    Enum.filter(attempts, &(Map.get(&1, :status) == :claimed))
  end

  defp runtime_current_node_ids(%Run{status: status}, _nodes, _fallback_current_node_id)
       when status in @terminal_statuses do
    []
  end

  defp runtime_current_node_ids(%Run{} = run, nodes, fallback_current_node_id) do
    active_node_ids =
      nodes
      |> Enum.filter(&runtime_current_node?(run, &1))
      |> Enum.map(& &1.id)

    case active_node_ids do
      [] -> List.wrap(fallback_current_node_id)
      node_ids -> node_ids
    end
  end

  defp runtime_current_node?(%Run{}, %Node{status: status})
       when status in [:pending, :running, :retrying, :paused] do
    true
  end

  defp runtime_current_node?(%Run{}, %Node{}), do: false

  defp mark_current_nodes(nodes, current_node_ids) do
    current_node_ids = MapSet.new(current_node_ids)

    Enum.map(nodes, fn %Node{} = node ->
      %Node{node | current?: MapSet.member?(current_node_ids, node.id)}
    end)
  end

  defp ordered_node_ids(nil, step_sources, step_id_fun) do
    step_sources
    |> Enum.map(step_id_fun)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp ordered_node_ids(definition, step_sources, step_id_fun) do
    declared_ids =
      definition
      |> Definition.inspect_steps()
      |> Enum.map(&normalize_id(&1.step))

    extra_ids =
      step_sources
      |> Enum.map(step_id_fun)
      |> Enum.reject(&(is_nil(&1) or &1 in declared_ids))
      |> Enum.uniq()

    declared_ids ++ extra_ids
  end

  defp runtime_step_id(nil), do: nil
  defp runtime_step_id(step_state), do: normalize_id(step_state.step)

  defp snapshot_step_id(step_source) when is_map(step_source) do
    step_source
    |> Map.get(:step)
    |> normalize_id()
  end

  defp snapshot_step_id(_step_source), do: nil

  defp current_node_id(_step, status) when status in @terminal_statuses, do: nil
  defp current_node_id(step, _status), do: normalize_id(step)

  defp terminal_status?(status), do: status in @terminal_statuses

  defp load_definition(workflow) when is_atom(workflow) do
    case Definition.load(workflow) do
      {:ok, definition} -> definition
      {:error, _reason} -> nil
    end
  end

  defp load_definition(workflow) when is_binary(workflow) do
    case Definition.load_serialized(workflow) do
      {:ok, _workflow, definition} -> definition
      {:error, _reason} -> nil
    end
  end

  defp load_definition(_workflow), do: nil

  defp normalize_id(nil), do: nil
  defp normalize_id(step) when is_atom(step), do: Atom.to_string(step)
  defp normalize_id(step) when is_binary(step), do: step

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end

  defp detail(true, value, _default), do: value
  defp detail(false, _value, default), do: default

  defp detail(include_details?, value) do
    detail(include_details?, value, nil)
  end

  defp sanitize_anomalies(anomalies) when is_list(anomalies) do
    Enum.map(
      anomalies,
      &Map.take(&1, [:source, :reason, :entry_type, :run_id, :step, :runnable_key])
    )
  end
end
