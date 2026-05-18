defmodule SquidMesh.ToolsTest do
  use ExUnit.Case, async: true

  alias SquidMesh.Tools
  alias SquidMesh.Tools.Error
  alias SquidMesh.Tools.Result

  defmodule SuccessfulAdapter do
    @behaviour SquidMesh.Tools.Adapter

    @impl SquidMesh.Tools.Adapter
    def invoke(request, context, _opts) do
      {:ok,
       %Result{
         adapter: __MODULE__,
         payload: %{request: request},
         metadata: %{context: context}
       }}
    end
  end

  defmodule InvalidAdapter do
    @behaviour SquidMesh.Tools.Adapter

    @impl SquidMesh.Tools.Adapter
    def invoke(_request, _context, _opts) do
      {:ok, %{payload: %{}}}
    end
  end

  describe "invoke/4" do
    test "returns normalized tool results from adapters" do
      assert {:ok, %Result{} = result} =
               Tools.invoke(SuccessfulAdapter, %{id: "req_123"}, %{run_id: "run_123"})

      assert result.adapter == SuccessfulAdapter
      assert result.payload == %{request: %{id: "req_123"}}
      assert result.metadata == %{context: %{run_id: "run_123"}}
    end

    test "normalizes invalid adapter responses into tool errors" do
      assert {:error, %Error{} = error} =
               Tools.invoke(InvalidAdapter, %{id: "req_123"}, %{run_id: "run_123"})

      assert error.adapter == InvalidAdapter
      assert error.kind == :adapter_contract
      assert error.retryable? == false
    end
  end
end
