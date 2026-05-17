defmodule SquidMesh.Runtime.StepExecutor.Execution do
  @moduledoc """
  Executes a prepared workflow step without mutating durable run state.

  This phase is intentionally narrow: it runs built-in steps or Jido actions
  using a previously prepared input snapshot and returns an execution result for
  the apply phase to persist.
  """

  alias SquidMesh.Run
  alias SquidMesh.Runtime.BuiltInStep
  alias SquidMesh.Runtime.StepExecutor.PreparedStep
  alias SquidMesh.Workflow.Definition, as: WorkflowDefinition

  @doc false
  @spec execute(PreparedStep.t(), pos_integer()) :: {:ok, map(), keyword()} | {:error, term()}
  def execute(
        %PreparedStep{
          step_name: _step_name,
          step: %{module: built_in_kind, opts: opts},
          input: input,
          run: run
        },
        _attempt_number
      )
      when built_in_kind in [:wait, :log, :pause, :approval] do
    BuiltInStep.execute(built_in_kind, opts, input, run)
  end

  def execute(
        %PreparedStep{
          step_name: step_name,
          definition: definition,
          config: config,
          step: %{module: action},
          input: input,
          run: %Run{} = run
        },
        attempt_number
      ) do
    boundary =
      case WorkflowDefinition.step_transaction_boundary(definition, step_name) do
        {:ok, transaction_boundary} -> transaction_boundary
        {:error, _reason} -> nil
      end

    execute_action(
      config.repo,
      boundary,
      action,
      input,
      step_context(run, step_name, attempt_number)
    )
  end

  defp execute_action(repo, :repo, action, input, context) do
    case repo.transaction(fn ->
           action
           |> run_action_with_jido(input, context, timeout: 0)
           |> rollback_action_error(repo)
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  defp execute_action(_repo, nil, action, input, context) do
    run_action_with_jido(action, input, context, [])
  end

  defp rollback_action_error({:ok, _output, _opts} = ok, _repo), do: ok
  defp rollback_action_error({:error, reason}, repo), do: repo.rollback(reason)

  defp step_context(%Run{} = run, step_name, attempt_number) do
    %{
      run_id: run.id,
      workflow: run.workflow,
      step: step_name,
      attempt: attempt_number,
      state: Map.merge(run.payload || %{}, run.context || %{})
    }
  end

  defp run_action_with_jido(action, input, context, opts) do
    opts = Keyword.put_new(opts, :max_retries, 0)
    {action, input} = action_input(action, input)

    case Jido.Exec.run(action, input, context, opts) do
      {:ok, output} when is_map(output) -> {:ok, output, []}
      {:ok, output, extras} when is_map(output) and is_list(extras) -> {:ok, output, extras}
      {:ok, output, _extras} when is_map(output) -> {:ok, output, []}
      {:error, reason} -> {:error, reason}
    end
  end

  defp action_input(action, input) do
    if SquidMesh.Step.native_step?(action) do
      {SquidMesh.Step.Action, %{step: action, input: input}}
    else
      {action, input}
    end
  end
end
