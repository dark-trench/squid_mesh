defmodule BedrockMinimalHostApp.ActionRegistryTest do
  use ExUnit.Case, async: true

  alias BedrockMinimalHostApp.Steps
  alias BedrockMinimalHostApp.Workflows.PaymentRecovery

  test "validates runtime-authored specs through host-owned action keys" do
    spec = %SquidMesh.Workflow.Spec{
      workflow: BedrockMinimalHostApp.RuntimeAuthoredPaymentRecovery,
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: [
            %{name: :account_id, type: :string, opts: []},
            %{name: :invoice_id, type: :string, opts: []}
          ]
        }
      ],
      payload: [
        %{name: :account_id, type: :string, opts: []},
        %{name: :invoice_id, type: :string, opts: []}
      ],
      steps: [
        %{name: :load_invoice, action: "payment.load_invoice", opts: []},
        %{name: :notify_customer, action: "payment.notify_customer", opts: []}
      ],
      transitions: [
        %{from: :load_invoice, on: :ok, to: :notify_customer},
        %{from: :notify_customer, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:load_invoice],
      initial_step: :load_invoice,
      entry_step: :load_invoice
    }

    registry = %{
      "payment.load_invoice" => Steps.LoadInvoice,
      "payment.notify_customer" => Steps.NotifyCustomer
    }

    assert :ok = SquidMesh.Workflow.validate_spec(spec, action_registry: registry)

    assert {:ok, resolved} =
             SquidMesh.Workflow.resolve_spec_actions(spec, action_registry: registry)

    assert Enum.map(resolved.steps, &{&1.name, &1.module, &1.metadata.action}) == [
             {:load_invoice, Steps.LoadInvoice, "payment.load_invoice"},
             {:notify_customer, Steps.NotifyCustomer, "payment.notify_customer"}
           ]
  end

  test "compiled payment recovery workflow exposes numeric gateway routing condition" do
    assert {:ok, spec} = SquidMesh.Workflow.to_spec(PaymentRecovery)

    assert Enum.any?(spec.transitions, fn
             %{
               from: :check_gateway_status,
               on: :ok,
               to: :notify_customer,
               condition: %{path: [:gateway_check, :status_code], greater_than: 199}
             } ->
               true

             _transition ->
               false
           end)
  end
end
