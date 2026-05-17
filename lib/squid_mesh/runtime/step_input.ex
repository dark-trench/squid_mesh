defmodule SquidMesh.Runtime.StepInput do
  @moduledoc """
  Step-execution input normalization for the runtime.

  This module keeps payload/context merging and identifier normalization out of
  the main executor flow.
  """

  alias SquidMesh.Run
  alias SquidMesh.StepRunStore
  alias SquidMesh.Workflow.Definition, as: WorkflowDefinition

  @type expected_step :: atom() | String.t() | nil
  @type input_mapping :: [atom()] | nil

  @doc false
  @spec deserialize_expected_step(expected_step()) ::
          {:ok, atom() | nil} | {:error, {:invalid_step, String.t()}}
  def deserialize_expected_step(nil), do: {:ok, nil}
  def deserialize_expected_step(step) when is_atom(step), do: {:ok, step}

  def deserialize_expected_step(step) when is_binary(step) do
    {:ok, String.to_existing_atom(step)}
  rescue
    ArgumentError -> {:error, {:invalid_step, step}}
  end

  @doc false
  @spec deserialize_expected_step(expected_step(), map()) ::
          {:ok, atom() | nil} | {:error, {:invalid_step, String.t()}}
  def deserialize_expected_step(nil, _definition), do: {:ok, nil}
  def deserialize_expected_step(step, _definition) when is_atom(step), do: {:ok, step}

  def deserialize_expected_step(step, definition) when is_binary(step) do
    case WorkflowDefinition.deserialize_step(definition, step) do
      resolved_step when is_atom(resolved_step) -> {:ok, resolved_step}
      _other -> {:error, {:invalid_step, step}}
    end
  end

  @doc false
  @spec build_step_input(Run.t(), input_mapping()) :: map()
  def build_step_input(%Run{payload: payload, context: context}, input_mapping \\ nil) do
    payload
    |> Kernel.||(%{})
    |> Map.merge(context || %{})
    |> normalize_map_keys()
    |> apply_input_mapping(input_mapping)
  end

  @doc false
  @spec build_dependency_step_input(module(), Run.t(), input_mapping()) :: map()
  def build_dependency_step_input(repo, %Run{id: run_id} = run, input_mapping \\ nil) do
    run
    |> build_step_input()
    |> merge_completed_outputs(StepRunStore.completed_outputs(repo, run_id))
    |> apply_input_mapping(input_mapping)
  end

  @doc false
  @spec normalize_map_keys(map()) :: map()
  def normalize_map_keys(map) when is_map(map) do
    Map.new(map, fn
      {key, value} when is_binary(key) ->
        {to_existing_atom(key), normalize_value(value)}

      {key, value} ->
        {key, normalize_value(value)}
    end)
  end

  defp normalize_value(%_{} = value), do: value
  defp normalize_value(value) when is_map(value), do: normalize_map_keys(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value), do: value

  defp merge_completed_outputs(input, outputs) do
    Enum.reduce(outputs, input, fn output, acc -> Map.merge(acc, normalize_map_keys(output)) end)
  end

  defp apply_input_mapping(input, nil), do: input

  defp apply_input_mapping(input, input_mapping) when is_list(input_mapping),
    do: Map.take(input, input_mapping)

  defp to_existing_atom(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end
end
