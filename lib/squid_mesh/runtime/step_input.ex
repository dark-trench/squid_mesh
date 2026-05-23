defmodule SquidMesh.Runtime.StepInput do
  @moduledoc """
  Step-execution input normalization for the runtime.

  This module keeps payload/context merging and identifier normalization out of
  the main executor flow.
  """

  alias SquidMesh.Run
  alias SquidMesh.Steps
  alias SquidMesh.Workflow.InputMapping

  @type expected_step :: atom() | String.t() | nil
  @type input_mapping :: InputMapping.t() | nil

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
    case SquidMesh.Workflow.Definition.deserialize_step(definition, step) do
      resolved_step when is_atom(resolved_step) -> {:ok, resolved_step}
      _ignored -> {:error, {:invalid_step, step}}
    end
  end

  @doc false
  @spec build_step_input(Run.t(), input_mapping()) :: map()
  def build_step_input(%Run{payload: payload, context: context}, input_mapping \\ nil) do
    case resolve_step_input(%Run{payload: payload, context: context}, input_mapping) do
      {:ok, input} -> input
      {:error, reason} -> raise ArgumentError, "invalid step input mapping: #{inspect(reason)}"
    end
  end

  @doc false
  @spec resolve_step_input(Run.t(), input_mapping()) ::
          {:ok, map()} | {:error, {:missing_input_path, map()} | {:invalid_input_mapping, term()}}
  def resolve_step_input(%Run{payload: payload, context: context}, input_mapping \\ nil) do
    payload
    |> Kernel.||(%{})
    |> Map.merge(context || %{})
    |> apply_input_mapping(input_mapping)
  end

  @doc false
  @spec build_dependency_step_input(module(), Run.t(), input_mapping()) :: map()
  def build_dependency_step_input(repo, %Run{} = run, input_mapping \\ nil) do
    case resolve_dependency_step_input(repo, run, input_mapping) do
      {:ok, input} -> input
      {:error, reason} -> raise ArgumentError, "invalid step input mapping: #{inspect(reason)}"
    end
  end

  @doc false
  @spec resolve_dependency_step_input(module(), Run.t(), input_mapping()) ::
          {:ok, map()} | {:error, {:missing_input_path, map()} | {:invalid_input_mapping, term()}}
  def resolve_dependency_step_input(repo, %Run{id: run_id} = run, input_mapping \\ nil) do
    run
    |> build_step_input()
    |> merge_completed_outputs(Steps.Store.completed_outputs(repo, run_id))
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

  defp normalize_value(%_struct{} = value), do: value
  defp normalize_value(value) when is_map(value), do: normalize_map_keys(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value), do: value

  defp merge_completed_outputs(input, outputs) do
    Enum.reduce(outputs, input, fn output, acc -> Map.merge(acc, normalize_map_keys(output)) end)
  end

  @doc false
  @spec apply_input_mapping(map(), input_mapping()) ::
          {:ok, map()} | {:error, {:missing_input_path, map()} | {:invalid_input_mapping, term()}}
  def apply_input_mapping(input, input_mapping) when is_map(input) do
    input
    |> normalize_map_keys()
    |> InputMapping.apply(input_mapping)
  end

  @doc false
  @spec input_mapping_error?(term()) :: boolean()
  def input_mapping_error?({:missing_input_path, details}) when is_map(details), do: true
  def input_mapping_error?(_reason), do: false

  @doc false
  @spec input_mapping_error_to_map({:missing_input_path, map()}) :: map()
  def input_mapping_error_to_map({:missing_input_path, details}) when is_map(details) do
    %{
      message: "missing mapped input path",
      code: "missing_input_path",
      target: to_error_segment(Map.fetch!(details, :target)),
      path: Enum.map(Map.fetch!(details, :path), &to_error_segment/1),
      missing_at: Enum.map(Map.fetch!(details, :missing_at), &to_error_segment/1),
      retryable?: false
    }
  end

  defp to_error_segment(segment) when is_atom(segment), do: Atom.to_string(segment)
  defp to_error_segment(segment), do: to_string(segment)

  defp to_existing_atom(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end
end
