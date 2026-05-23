defmodule SquidMesh.Workflow.TransitionCondition do
  @moduledoc false

  @type json_value ::
          nil
          | boolean()
          | number()
          | String.t()
          | [json_value()]
          | %{optional(String.t()) => json_value()}
  @type t :: %{required(:path) => [atom()], required(:equals) => json_value()}
  @type error :: :invalid_condition

  @doc false
  @spec normalize(term()) :: {:ok, t()} | {:error, error()}
  def normalize(condition) when is_list(condition) or is_map(condition) do
    with {:ok, condition} <- condition_map(condition),
         {:ok, expected} <- fetch_condition_value(condition),
         true <- json_value?(expected),
         {:ok, normalized_path} <- normalize_path(condition_path(condition)) do
      {:ok, %{path: normalized_path, equals: expected}}
    else
      _invalid -> {:error, :invalid_condition}
    end
  end

  def normalize(_condition), do: {:error, :invalid_condition}

  @doc false
  @spec normalize!(term()) :: t()
  def normalize!(condition) do
    case normalize(condition) do
      {:ok, normalized} -> normalized
      {:error, :invalid_condition} -> raise ArgumentError, "invalid transition condition"
    end
  end

  @doc false
  @spec matches?(map(), t() | map()) :: boolean()
  def matches?(context, condition) when is_map(context) do
    case normalize(condition) do
      {:ok, %{path: path, equals: expected}} ->
        case fetch_path(context, path) do
          {:ok, ^expected} -> true
          {:ok, _other} -> false
          :error -> false
        end

      {:error, :invalid_condition} ->
        false
    end
  end

  def matches?(_context, _condition), do: false

  @doc false
  @spec serialize(t() | map() | nil) :: map() | nil
  def serialize(nil), do: nil

  def serialize(condition) do
    case normalize(condition) do
      {:ok, %{path: path, equals: expected}} ->
        %{"path" => Enum.map(path, &Atom.to_string/1), "equals" => expected}

      {:error, :invalid_condition} ->
        nil
    end
  end

  @doc false
  @spec deserialize(map() | nil) :: t() | nil
  def deserialize(nil), do: nil

  def deserialize(condition) when is_map(condition) do
    path = Map.get(condition, :path) || Map.get(condition, "path")

    with true <- is_list(path),
         {:ok, normalized_path} <- deserialize_path(path),
         {:ok, normalized} <-
           normalize(%{
             path: normalized_path,
             equals: Map.get(condition, :equals, Map.get(condition, "equals"))
           }) do
      normalized
    else
      _invalid -> nil
    end
  end

  def deserialize(_condition), do: nil

  defp fetch_path(context, path) do
    Enum.reduce_while(path, {:ok, context}, fn segment, {:ok, current} ->
      case fetch_segment(current, segment) do
        {:ok, value} -> {:cont, {:ok, value}}
        :error -> {:halt, :error}
      end
    end)
  end

  defp fetch_segment(%{} = map, segment) when is_atom(segment) do
    case Map.fetch(map, segment) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(map, Atom.to_string(segment))
    end
  end

  defp fetch_segment(_value, _segment), do: :error

  defp condition_map(%{} = condition), do: {:ok, condition}

  defp condition_map(condition) when is_list(condition) do
    {:ok, Map.new(condition)}
  rescue
    ArgumentError -> {:error, :invalid_condition}
  end

  defp condition_path(condition), do: Map.get(condition, :path) || Map.get(condition, "path")

  defp fetch_condition_value(condition) do
    cond do
      Map.has_key?(condition, :equals) -> {:ok, Map.get(condition, :equals)}
      Map.has_key?(condition, "equals") -> {:ok, Map.get(condition, "equals")}
      true -> {:error, :invalid_condition}
    end
  end

  defp json_value?(nil), do: true
  defp json_value?(value) when is_boolean(value), do: true
  defp json_value?(value) when is_binary(value), do: true
  defp json_value?(value) when is_number(value), do: true
  defp json_value?(values) when is_list(values), do: Enum.all?(values, &json_value?/1)

  defp json_value?(%{} = value) do
    Enum.all?(value, fn
      {key, nested_value} when is_binary(key) -> json_value?(nested_value)
      _other -> false
    end)
  end

  defp json_value?(_value), do: false

  defp normalize_path(path) do
    if is_list(path) and path != [] do
      deserialize_path(path)
    else
      :error
    end
  end

  defp deserialize_path(path) do
    result =
      Enum.reduce_while(path, {:ok, []}, fn segment, {:ok, acc} ->
        case deserialize_segment(segment) do
          {:ok, atom} -> {:cont, {:ok, [atom | acc]}}
          :error -> {:halt, :error}
        end
      end)

    case result do
      {:ok, path} -> {:ok, Enum.reverse(path)}
      :error -> :error
    end
  end

  defp deserialize_segment(segment) when is_atom(segment), do: {:ok, segment}

  defp deserialize_segment(segment) when is_binary(segment) do
    {:ok, String.to_existing_atom(segment)}
  rescue
    ArgumentError -> :error
  end

  defp deserialize_segment(_segment), do: :error
end
