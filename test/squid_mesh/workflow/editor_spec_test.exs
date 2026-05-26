defmodule SquidMesh.Workflow.EditorSpecTest do
  use ExUnit.Case, async: true

  alias SquidMesh.Workflow.EditorSpec

  defmodule LoadInvoice do
    use SquidMesh.Step,
      name: :load_invoice,
      input_schema: [
        invoice_id: [type: :string, required: true]
      ],
      output_schema: [
        invoice: [type: :map, required: true]
      ]

    @impl SquidMesh.Step
    def run(_input, _context), do: {:ok, %{invoice: %{id: "inv_123"}}}
  end

  defmodule SendReminder do
    use SquidMesh.Step,
      name: :send_reminder,
      input_schema: [
        invoice: [type: :map, required: true]
      ],
      output_schema: [
        sent?: [type: :boolean, required: true]
      ]

    @impl SquidMesh.Step
    def run(_input, _context), do: {:ok, %{sent?: true}}
  end

  defmodule PaymentRecovery do
    use SquidMesh.Workflow

    workflow do
      version "2026-05-26.payment-recovery"

      trigger :manual do
        manual()

        payload do
          field :invoice_id, :string
          field :digest_date, :string, default: {:today, :iso8601}
        end
      end

      step :load_invoice, LoadInvoice, input: [:invoice_id], output: :invoice
      step :send_reminder, SendReminder, input: [:invoice], output: :reminder

      transition :load_invoice, on: :ok, to: :send_reminder
      transition :send_reminder, on: :ok, to: :complete
      transition :send_reminder, on: :error, to: :complete, recovery: :compensation
    end
  end

  describe "JSON-safe editor specs" do
    test "round-trips a representative workflow spec through JSON and previews its graph" do
      assert {:ok, spec} = SquidMesh.Workflow.to_spec(PaymentRecovery)

      round_tripped =
        spec
        |> EditorSpec.to_map()
        |> Jason.encode!()
        |> Jason.decode!()

      assert :ok = EditorSpec.validate_map(round_tripped)

      assert [
               %{"metadata" => %{"description" => nil}},
               %{"metadata" => %{"output_schema" => %{"sent?" => %{"required" => true}}}}
             ] = round_tripped["steps"]

      assert [
               %{"name" => "invoice_id"},
               %{"name" => "digest_date", "opts" => %{"default" => ["today", "iso8601"]}}
             ] = round_tripped["payload"]

      assert {:ok, graph} = EditorSpec.preview_graph(round_tripped)

      assert %{
               "source" => "workflow_spec",
               "status" => "draft",
               "workflow" => workflow,
               "definition_version" => "2026-05-26.payment-recovery",
               "nodes" => [
                 %{"id" => "load_invoice", "status" => "draft"},
                 %{"id" => "send_reminder", "status" => "draft"}
               ],
               "edges" => [
                 %{
                   "id" => "load_invoice:ok:send_reminder",
                   "from" => "load_invoice",
                   "to" => "send_reminder",
                   "type" => "transition",
                   "status" => "pending",
                   "outcome" => "ok"
                 },
                 %{
                   "id" => "send_reminder:ok:complete",
                   "from" => "send_reminder",
                   "to" => "complete",
                   "type" => "transition",
                   "status" => "pending",
                   "outcome" => "ok"
                 },
                 %{
                   "id" => "send_reminder:error:complete",
                   "from" => "send_reminder",
                   "to" => "complete",
                   "type" => "transition",
                   "status" => "pending",
                   "outcome" => "error",
                   "recovery" => "compensation"
                 }
               ]
             } = graph

      assert workflow =~ "PaymentRecovery"
    end

    test "rejects runtime-owned fields before previewing editor data" do
      assert {:ok, spec} = SquidMesh.Workflow.to_spec(PaymentRecovery)

      editor_map =
        spec
        |> EditorSpec.to_map()
        |> Map.put("definition_fingerprint", "sha256:abc")

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.validate_map(editor_map)

      assert %{
               path: [:definition_fingerprint],
               code: :runtime_owned_field,
               message: "definition_fingerprint is runtime-owned and cannot be edited",
               details: %{field: "definition_fingerprint"}
             } in errors

      assert {:error, {:invalid_workflow_editor_spec, ^errors}} =
               EditorSpec.preview_graph(editor_map)
    end

    test "returns stable field paths for invalid graph references" do
      assert {:ok, spec} = SquidMesh.Workflow.to_spec(PaymentRecovery)

      editor_map =
        spec
        |> EditorSpec.to_map()
        |> Map.put("transitions", [
          %{"from" => "load_invoice", "on" => "ok", "to" => "missing_step"}
        ])

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.validate_map(editor_map)

      assert %{
               path: [:transitions, 0, :to],
               code: :unknown_transition_target,
               message: "transition targets unknown step: missing_step",
               details: %{to: "missing_step"}
             } in errors
    end
  end
end
