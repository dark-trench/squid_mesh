defmodule SquidMesh.WorkflowTest do
  use ExUnit.Case, async: true

  alias __MODULE__.DependencyWorkflow
  alias __MODULE__.InvoiceReminder
  alias __MODULE__.MalformedSchemaStep
  alias __MODULE__.NativeStepContractWorkflow
  alias __MODULE__.NativeStepStructError

  defmodule NativeStepContractWorkflow.LoadAccount do
    use SquidMesh.Step,
      name: :load_account,
      description: "Loads account details",
      input_schema: [
        account_id: [type: :string, required: true]
      ],
      output_schema: [
        account: [type: :map, required: true]
      ]

    @impl SquidMesh.Step
    def run(_input, _context), do: {:ok, %{account: %{id: "acct_123"}}}
  end

  defmodule NativeStepContractWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :load_account, NativeStepContractWorkflow.LoadAccount

      transition :load_account, on: :ok, to: :complete
    end
  end

  defmodule MalformedSchemaStep do
    use SquidMesh.Step,
      name: :malformed_schema,
      input_schema: [
        account_id: :string
      ]

    @impl SquidMesh.Step
    def run(_input, _context), do: {:ok, %{}}
  end

  defmodule NativeStepStructError do
    defexception [:message, :code]
  end

  test "exposes a declarative workflow definition" do
    definition = InvoiceReminder.workflow_definition()

    assert definition.triggers == [
             %{
               name: :manual,
               type: :manual,
               config: %{},
               payload: [
                 %{name: :account_id, type: :string, opts: []},
                 %{name: :invoice_id, type: :string, opts: []}
               ]
             }
           ]

    assert definition.payload == [
             %{name: :account_id, type: :string, opts: []},
             %{name: :invoice_id, type: :string, opts: []}
           ]

    assert definition.steps == [
             %{name: :load_invoice, module: InvoiceReminder.LoadInvoice, opts: []},
             %{
               name: :send_email,
               module: InvoiceReminder.SendEmail,
               opts: [retry: [max_attempts: 3]]
             },
             %{name: :record_delivery, module: InvoiceReminder.RecordDelivery, opts: []}
           ]

    assert definition.transitions == [
             %{from: :load_invoice, on: :ok, to: :send_email},
             %{from: :send_email, on: :ok, to: :record_delivery},
             %{from: :record_delivery, on: :ok, to: :complete}
           ]

    assert definition.retries == [
             %{step: :send_email, opts: [max_attempts: 3]}
           ]

    assert definition.entry_step == :load_invoice
  end

  test "exposes the workflow contract shape" do
    assert InvoiceReminder.__workflow__(:contract) == %{
             required: [:trigger, :step],
             optional: [:transition]
           }
  end

  test "supports dependency-based step declarations with multiple entry steps" do
    definition = DependencyWorkflow.workflow_definition()

    assert definition.steps == [
             %{name: :load_account, module: DependencyWorkflow.LoadAccount, opts: []},
             %{name: :load_invoice, module: DependencyWorkflow.LoadInvoice, opts: []},
             %{
               name: :send_email,
               module: DependencyWorkflow.SendEmail,
               opts: [after: [:load_account, :load_invoice]]
             }
           ]

    assert definition.entry_steps == [:load_account, :load_invoice]
    assert definition.initial_step == :load_account
    assert definition.entry_step == nil
  end

  test "supports explicit step input and output mapping options" do
    module =
      compile_module("""
      defmodule WorkflowWithStepMappings do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()

            payload do
              field :account_id, :string
              field :invoice_id, :string
            end
          end

          step :load_account, WorkflowWithStepMappings.LoadAccount,
            input: [:account_id],
            output: :account

          transition :load_account, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().steps == [
             %{
               name: :load_account,
               module: Module.safe_concat(module, LoadAccount),
               opts: [input: [:account_id], output: :account]
             }
           ]
  end

  test "stores native Squid Mesh step contract metadata in the workflow definition" do
    assert [
             %{
               name: :load_account,
               module: step_module,
               opts: [],
               metadata: %{
                 contract: :squid_mesh_step,
                 name: :load_account,
                 description: "Loads account details",
                 input_schema: [account_id: [type: :string, required: true]],
                 output_schema: [account: [type: :map, required: true]]
               }
             }
           ] = NativeStepContractWorkflow.workflow_definition().steps

    assert step_module == NativeStepContractWorkflow.LoadAccount

    assert [
             %SquidMesh.Workflow.StepSpec{
               metadata: %{
                 contract: :squid_mesh_step,
                 name: :load_account,
                 input_schema: [account_id: [type: :string, required: true]]
               }
             }
           ] = SquidMesh.Workflow.Info.steps(NativeStepContractWorkflow)
  end

  test "resolves native step metadata when a step module is compiled after the workflow" do
    compiled_modules =
      Code.compile_string(
        """
        defmodule WorkflowWithLateNativeStep do
          use SquidMesh.Workflow

          workflow do
            trigger :manual do
              manual()
            end

            step :load_account, WorkflowWithLateNativeStep.LoadAccount

            transition :load_account, on: :ok, to: :complete
          end
        end

        defmodule WorkflowWithLateNativeStep.LoadAccount do
          use SquidMesh.Step,
            name: :load_account,
            description: "Loads late account details",
            input_schema: [
              account_id: [type: :string, required: true]
            ],
            output_schema: [
              account: [type: :map, required: true]
            ]

          @impl SquidMesh.Step
          def run(_input, _context), do: {:ok, %{account: %{id: "acct_late"}}}
        end
        """,
        "test/support/late_native_step_workflow.exs"
      )

    workflow = compiled_module!(compiled_modules, WorkflowWithLateNativeStep)
    step_module = compiled_module!(compiled_modules, WorkflowWithLateNativeStep.LoadAccount)

    assert [
             %{
               name: :load_account,
               module: ^step_module,
               metadata: %{
                 contract: :squid_mesh_step,
                 description: "Loads late account details",
                 input_schema: [account_id: [type: :string, required: true]]
               }
             }
           ] = workflow.workflow_definition().steps
  end

  test "returns structured errors for malformed native step schemas" do
    assert {:error,
            %{
              message: "native step input schema is invalid",
              retryable?: false
            }} = SquidMesh.Step.validate_input(MalformedSchemaStep, %{})
  end

  test "normalizes native step struct errors before adding retry metadata" do
    assert {:error,
            %{
              message: "declined",
              code: "card_declined",
              type: "SquidMesh.WorkflowTest.NativeStepStructError",
              retryable?: false
            }} =
             SquidMesh.Step.normalize_result(
               {:error, %NativeStepStructError{message: "declined", code: "card_declined"}}
             )
  end

  test "supports explicit irreversible and non-compensatable step markers" do
    module =
      compile_module("""
      defmodule WorkflowWithRecoveryMarkers do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithRecoveryMarkers.CapturePayment, irreversible: true)
          step(:send_receipt, WorkflowWithRecoveryMarkers.SendReceipt, compensatable: false)

          transition(:capture_payment, on: :ok, to: :send_receipt)
          transition(:send_receipt, on: :ok, to: :complete)
        end
      end
      """)

    definition = module.workflow_definition()

    assert SquidMesh.Workflow.Definition.step_recovery_policy(definition, :capture_payment) ==
             {:ok,
              %{
                irreversible?: true,
                compensatable?: false,
                replay: :manual_review_required,
                recovery: :manual_intervention
              }}

    assert SquidMesh.Workflow.Definition.step_recovery_policy(definition, :send_receipt) ==
             {:ok,
              %{
                irreversible?: false,
                compensatable?: false,
                replay: :manual_review_required,
                recovery: :manual_intervention
              }}
  end

  test "supports explicit step compensation callbacks" do
    module =
      compile_module("""
      defmodule WorkflowWithCompensation do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:reserve_inventory, WorkflowWithCompensation.ReserveInventory,
            compensate: WorkflowWithCompensation.ReleaseInventory
          )

          transition(:reserve_inventory, on: :ok, to: :complete)
        end
      end
      """)

    definition = module.workflow_definition()

    assert SquidMesh.Workflow.Definition.step_compensation_callback(
             definition,
             :reserve_inventory
           ) ==
             {:ok, Module.safe_concat(module, ReleaseInventory)}
  end

  test "supports repo transaction boundaries on local step groups" do
    module =
      compile_module("""
      defmodule WorkflowWithTransactionalStepGroup do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:write_local_records, WorkflowWithTransactionalStepGroup.WriteLocalRecords,
            transaction: :repo
          )

          transition(:write_local_records, on: :ok, to: :complete)
        end
      end
      """)

    definition = module.workflow_definition()

    assert [
             %{
               name: :write_local_records,
               module: WorkflowWithTransactionalStepGroup.WriteLocalRecords,
               opts: [transaction: :repo]
             }
           ] = definition.steps

    assert SquidMesh.Workflow.Definition.step_transaction_boundary(
             definition,
             :write_local_records
           ) == {:ok, :repo}
  end

  test "supports explicit compensation and undo error transition markers" do
    module =
      compile_module("""
      defmodule WorkflowWithFailureRecoveryMarkers do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithFailureRecoveryMarkers.CapturePayment)
          step(:issue_credit, WorkflowWithFailureRecoveryMarkers.IssueCredit)
          step(:reserve_inventory, WorkflowWithFailureRecoveryMarkers.ReserveInventory)
          step(:release_inventory, WorkflowWithFailureRecoveryMarkers.ReleaseInventory)

          transition(:capture_payment, on: :error, to: :issue_credit, recovery: :compensation)
          transition(:issue_credit, on: :ok, to: :reserve_inventory)
          transition(:reserve_inventory, on: :error, to: :release_inventory, recovery: :undo)
          transition(:release_inventory, on: :ok, to: :complete)
        end
      end
      """)

    assert [
             %{
               from: :capture_payment,
               on: :error,
               to: :issue_credit,
               recovery: :compensation
             },
             %{from: :issue_credit, on: :ok, to: :reserve_inventory},
             %{from: :reserve_inventory, on: :error, to: :release_inventory, recovery: :undo},
             %{from: :release_inventory, on: :ok, to: :complete}
           ] = module.workflow_definition().transitions
  end

  test "normalizes persisted irreversible policy as non-compensatable" do
    assert SquidMesh.Workflow.Definition.normalize_recovery_policy(%{
             "irreversible?" => true,
             "compensatable?" => true,
             "replay" => "manual_review_required",
             "recovery" => "manual_intervention"
           }) == %{
             irreversible?: true,
             compensatable?: false,
             replay: :manual_review_required,
             recovery: :manual_intervention
           }
  end

  test "normalizes persisted failure recovery decisions" do
    assert SquidMesh.Workflow.Definition.normalize_recovery_policy(%{
             "irreversible?" => false,
             "compensatable?" => true,
             "replay" => "allowed",
             "recovery" => "automatic",
             "failure" => %{"strategy" => "undo", "target" => "release_inventory"}
           }) == %{
             irreversible?: false,
             compensatable?: true,
             replay: :allowed,
             recovery: :automatic,
             failure: %{strategy: :undo, target: "release_inventory"}
           }
  end

  test "supports introspection of definition segments" do
    assert InvoiceReminder.__workflow__(:steps) == InvoiceReminder.workflow_definition().steps
    assert InvoiceReminder.__workflow__(:payload) == InvoiceReminder.workflow_definition().payload

    assert InvoiceReminder.__workflow__(:triggers) ==
             InvoiceReminder.workflow_definition().triggers

    assert InvoiceReminder.__workflow__(:transitions) ==
             InvoiceReminder.workflow_definition().transitions

    assert InvoiceReminder.__workflow__(:retries) == InvoiceReminder.workflow_definition().retries
    assert InvoiceReminder.__workflow__(:entry_step) == :load_invoice
  end

  test "exposes dependency entry steps for introspection" do
    assert DependencyWorkflow.__workflow__(:entry_steps) == [:load_account, :load_invoice]
    assert DependencyWorkflow.__workflow__(:initial_step) == :load_account
    assert DependencyWorkflow.__workflow__(:entry_step) == nil
  end

  test "waits when the current dependency phase already has a failed sibling" do
    definition = DependencyWorkflow.workflow_definition()

    assert {:wait, [:load_invoice]} =
             SquidMesh.Workflow.Definition.dependency_progress(definition, %{
               load_account: :completed,
               load_invoice: :failed
             })
  end

  test "fails when no steps are declared" do
    assert_compile_error(
      """
      defmodule WorkflowWithoutSteps do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()

            payload do
              field :account_id, :string
            end
          end
        end
      end
      """,
      "at least one step is required"
    )
  end

  test "fails when step names are duplicated" do
    assert_compile_error(
      """
      defmodule WorkflowWithDuplicateSteps do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithDuplicateSteps.SendEmail
          step :send_email, WorkflowWithDuplicateSteps.RecordDelivery
        end
      end
      """,
      "duplicate step names: :send_email"
    )
  end

  test "fails when step retry policy is malformed" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidRetry do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidRetry.SendEmail, retry: [max_attempts: 0]
        end
      end
      """,
      "retry for :send_email must define a positive :max_attempts"
    )
  end

  test "does not expose retries when no step config defines them" do
    module =
      compile_module("""
      defmodule WorkflowWithoutRetries do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithoutRetries.SendEmail
          transition :send_email, on: :ok, to: :complete
        end
      end
      """)

    assert module.__workflow__(:retries) == []
  end

  test "fails when step retry is not a keyword list" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidRetryShape do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidRetryShape.SendEmail, retry: 3
        end
      end
      """,
      "retry for :send_email must define a positive :max_attempts"
    )
  end

  test "fails when retry backoff configuration is invalid" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidRetryBackoff do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidRetryBackoff.SendEmail,
            retry: [max_attempts: 3, backoff: [type: :exponential, min: 0, max: 1_000]]
        end
      end
      """,
      "retry for :send_email defines an invalid :backoff option"
    )
  end

  test "fails when step input mapping is not a non-empty atom list" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidStepInput do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidStepInput.SendEmail, input: "account_id"
        end
      end
      """,
      "step :send_email defines an invalid :input mapping"
    )
  end

  test "fails when step output mapping is not an atom" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidStepOutput do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidStepOutput.SendEmail, output: [:delivery]
        end
      end
      """,
      "step :send_email defines an invalid :output mapping"
    )
  end

  test "fails when irreversible and compensatable markers conflict" do
    assert_compile_error(
      """
      defmodule WorkflowWithConflictingRecoveryMarkers do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithConflictingRecoveryMarkers.CapturePayment,
            irreversible: true,
            compensatable: true
          )

          transition(:capture_payment, on: :ok, to: :complete)
        end
      end
      """,
      "step :capture_payment cannot be both irreversible and compensatable"
    )
  end

  test "fails when a compensation callback is declared for an irreversible step" do
    assert_compile_error(
      """
      defmodule WorkflowWithConflictingCompensation do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithConflictingCompensation.CapturePayment,
            irreversible: true,
            compensate: WorkflowWithConflictingCompensation.VoidPayment
          )

          transition(:capture_payment, on: :ok, to: :complete)
        end
      end
      """,
      "step :capture_payment cannot declare :compensate when it is irreversible or non-compensatable"
    )
  end

  test "fails when a compensation callback is a built-in step kind" do
    assert_compile_error(
      """
      defmodule WorkflowWithBuiltInCompensation do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:reserve_inventory, WorkflowWithBuiltInCompensation.ReserveInventory,
            compensate: :log
          )

          transition(:reserve_inventory, on: :ok, to: :complete)
        end
      end
      """,
      "step :reserve_inventory defines an invalid :compensate callback"
    )
  end

  test "fails when a compensation callback is not a module atom" do
    assert_compile_error(
      """
      defmodule WorkflowWithNonModuleCompensation do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:reserve_inventory, WorkflowWithNonModuleCompensation.ReserveInventory,
            compensate: :release_inventory
          )

          transition(:reserve_inventory, on: :ok, to: :complete)
        end
      end
      """,
      "step :reserve_inventory defines an invalid :compensate callback"
    )
  end

  test "does not report recovery marker conflicts for invalid marker shapes" do
    error =
      assert_raise CompileError, fn ->
        Code.compile_string(
          """
          defmodule WorkflowWithInvalidRecoveryMarkerShape do
            use SquidMesh.Workflow

            workflow do
              trigger :manual do
                manual()
              end

              step(:capture_payment, WorkflowWithInvalidRecoveryMarkerShape.CapturePayment,
                irreversible: :yes,
                compensatable: true
              )

              transition(:capture_payment, on: :ok, to: :complete)
            end
          end
          """,
          "test/support/invalid_workflow.exs"
        )
      end

    message = Exception.message(error)

    assert String.contains?(
             message,
             "step :capture_payment defines an invalid :irreversible marker"
           )

    refute String.contains?(message, "cannot be both irreversible and compensatable")
  end

  test "fails when transaction boundary markers are invalid" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidTransactionBoundary do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:write_local_records, WorkflowWithInvalidTransactionBoundary.WriteLocalRecords,
            transaction: :database
          )

          transition(:write_local_records, on: :ok, to: :complete)
        end
      end
      """,
      "step :write_local_records defines an invalid :transaction boundary"
    )
  end

  test "fails when built-in steps declare transaction boundaries" do
    assert_compile_error(
      """
      defmodule WorkflowWithBuiltInTransactionBoundary do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:record_message, :log, message: "local work", transaction: :repo)

          transition(:record_message, on: :ok, to: :complete)
        end
      end
      """,
      "built-in step :record_message cannot declare a :transaction boundary"
    )
  end

  test "fails when error transition recovery markers are invalid" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidFailureRecoveryMarker do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithInvalidFailureRecoveryMarker.CapturePayment)
          step(:issue_credit, WorkflowWithInvalidFailureRecoveryMarker.IssueCredit)

          transition(:capture_payment, on: :error, to: :issue_credit, recovery: :rollback)
          transition(:issue_credit, on: :ok, to: :complete)
        end
      end
      """,
      "transition from :capture_payment defines unsupported recovery marker :rollback"
    )
  end

  test "fails when success transitions define recovery markers" do
    assert_compile_error(
      """
      defmodule WorkflowWithSuccessRecoveryMarker do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithSuccessRecoveryMarker.CapturePayment)
          step(:issue_credit, WorkflowWithSuccessRecoveryMarker.IssueCredit)

          transition(:capture_payment, on: :ok, to: :issue_credit, recovery: :undo)
          transition(:issue_credit, on: :ok, to: :complete)
        end
      end
      """,
      "transition from :capture_payment can only define recovery markers for :error outcomes"
    )
  end

  test "fails when a workflow defines multiple entry steps" do
    assert_compile_error(
      """
      defmodule WorkflowWithMultipleEntrySteps do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithMultipleEntrySteps.LoadInvoice
          step :send_email, WorkflowWithMultipleEntrySteps.SendEmail
        end
      end
      """,
      "workflow must define exactly one entry step"
    )
  end

  test "allows multiple entry steps when dependency execution is declared" do
    module =
      compile_module("""
      defmodule WorkflowWithMultipleDependencyRoots do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_account, WorkflowWithMultipleDependencyRoots.LoadAccount
          step :load_invoice, WorkflowWithMultipleDependencyRoots.LoadInvoice
          step :send_email, WorkflowWithMultipleDependencyRoots.SendEmail,
            after: [:load_account, :load_invoice]
        end
      end
      """)

    assert module.__workflow__(:entry_steps) == [:load_account, :load_invoice]
  end

  test "fails when a workflow defines no entry step" do
    assert_compile_error(
      """
      defmodule WorkflowWithoutEntryStep do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithoutEntryStep.LoadInvoice
          step :send_email, WorkflowWithoutEntryStep.SendEmail

          transition :load_invoice, on: :ok, to: :send_email
          transition :send_email, on: :ok, to: :load_invoice
        end
      end
      """,
      "workflow must define exactly one entry step"
    )
  end

  test "fails when a dependency references an unknown step" do
    assert_compile_error(
      """
      defmodule WorkflowWithUnknownDependency do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithUnknownDependency.LoadInvoice
          step :send_email, WorkflowWithUnknownDependency.SendEmail, after: [:missing_step]
        end
      end
      """,
      "step :send_email depends on unknown step :missing_step"
    )
  end

  test "entry_steps!/2 raises a dependency-specific error when no root steps exist" do
    definition = %{
      steps: [
        %{name: :load_account, opts: [after: [:send_email]]},
        %{name: :send_email, opts: [after: [:load_account]]}
      ],
      transitions: []
    }

    assert_raise CompileError,
                 ~r/dependency-based workflow must define at least one root step/,
                 fn ->
                   SquidMesh.Workflow.Validation.entry_steps!(definition, __ENV__)
                 end
  end

  test "fails when dependency declarations contain a cycle" do
    assert_compile_error(
      """
      defmodule WorkflowWithDependencyCycle do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithDependencyCycle.LoadInvoice, after: [:send_email]
          step :send_email, WorkflowWithDependencyCycle.SendEmail, after: [:load_invoice]
        end
      end
      """,
      "workflow dependency graph must be acyclic"
    )
  end

  test "fails when a workflow mixes dependency joins with transitions" do
    assert_compile_error(
      """
      defmodule WorkflowWithMixedProgression do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_account, WorkflowWithMixedProgression.LoadAccount
          step :load_invoice, WorkflowWithMixedProgression.LoadInvoice
          step :prepare_notification, WorkflowWithMixedProgression.PrepareNotification,
            after: [:load_account, :load_invoice]
          step :record_delivery, WorkflowWithMixedProgression.RecordDelivery

          transition :prepare_notification, on: :ok, to: :record_delivery
          transition :record_delivery, on: :ok, to: :complete
        end
      end
      """,
      "dependency-based workflows cannot declare transitions"
    )
  end

  test "fails when :after is not a list of step atoms" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidAfterShape do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithInvalidAfterShape.LoadInvoice
          step :send_email, WorkflowWithInvalidAfterShape.SendEmail, after: "load_invoice"
        end
      end
      """,
      "step :send_email defines an invalid :after dependency list"
    )
  end

  test "fails when :after is empty" do
    assert_compile_error(
      """
      defmodule WorkflowWithEmptyAfter do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithEmptyAfter.LoadInvoice
          step :send_email, WorkflowWithEmptyAfter.SendEmail, after: []
        end
      end
      """,
      "step :send_email defines an invalid :after dependency list"
    )
  end

  test "fails when no triggers are declared" do
    assert_compile_error(
      """
      defmodule WorkflowWithoutTriggers do
        use SquidMesh.Workflow

        workflow do
          step :load_invoice, WorkflowWithoutTriggers.LoadInvoice
        end
      end
      """,
      "at least one trigger is required"
    )
  end

  test "fails when a trigger does not define a type" do
    assert_compile_error(
      """
      defmodule WorkflowWithUntypedTrigger do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            payload do
              field :account_id, :string
            end
          end

          step :load_invoice, WorkflowWithUntypedTrigger.LoadInvoice
        end
      end
      """,
      "trigger :manual must define exactly one type"
    )
  end

  test "supports multiple triggers with independent payload contracts" do
    module =
      compile_module("""
      defmodule WorkflowWithMultipleTriggers do
        use SquidMesh.Workflow

        workflow do
          trigger :manual_digest do
            manual()

            payload do
              field :chat_id, :integer
            end
          end

          trigger :scheduled_digest do
            cron "0 9 * * *", timezone: "UTC"

            payload do
              field :window_start_at, :string, default: {:today, :iso8601}
            end
          end

          step :load_invoice, WorkflowWithMultipleTriggers.LoadInvoice
          transition :load_invoice, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().triggers == [
             %{
               name: :manual_digest,
               type: :manual,
               config: %{},
               payload: [%{name: :chat_id, type: :integer, opts: []}]
             },
             %{
               name: :scheduled_digest,
               type: :cron,
               config: %{expression: "0 9 * * *", timezone: "UTC"},
               payload: [
                 %{name: :window_start_at, type: :string, opts: [default: {:today, :iso8601}]}
               ]
             }
           ]

    assert module.workflow_definition().payload == [
             %{name: :chat_id, type: :integer, opts: []},
             %{name: :window_start_at, type: :string, opts: [default: {:today, :iso8601}]}
           ]
  end

  test "fails when triggers declare incompatible payload field types" do
    assert_compile_error(
      """
      defmodule WorkflowWithConflictingTriggerPayloads do
        use SquidMesh.Workflow

        workflow do
          trigger :manual_digest do
            manual()

            payload do
              field :chat_id, :integer
            end
          end

          trigger :scheduled_digest do
            cron "0 9 * * *", timezone: "UTC"

            payload do
              field :chat_id, :string
            end
          end

          step :load_invoice, WorkflowWithConflictingTriggerPayloads.LoadInvoice
          transition :load_invoice, on: :ok, to: :complete
        end
      end
      """,
      "payload field :chat_id defines conflicting types across triggers"
    )
  end

  test "fails when a trigger declares more than one type" do
    assert_compile_error(
      """
      defmodule WorkflowWithMultipleTriggerTypes do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
            cron "0 9 * * *", timezone: "UTC"
          end

          step :load_invoice, WorkflowWithMultipleTriggerTypes.LoadInvoice
        end
      end
      """,
      "trigger :manual must define exactly one type"
    )
  end

  test "exposes cron trigger metadata and payload defaults" do
    module =
      compile_module("""
      defmodule WorkflowWithCronTrigger do
        use SquidMesh.Workflow

        workflow do
          trigger :daily_standup do
            cron "0 9 * * 1-5", timezone: "America/Sao_Paulo"

            payload do
              field :team_id, :string, default: "backend"
              field :prompt_date, :string, default: {:today, :iso8601}
            end
          end

          step :load_team_members, WorkflowWithCronTrigger.LoadTeamMembers
          transition :load_team_members, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().triggers == [
             %{
               name: :daily_standup,
               type: :cron,
               config: %{
                 expression: "0 9 * * 1-5",
                 timezone: "America/Sao_Paulo"
               },
               payload: [
                 %{name: :team_id, type: :string, opts: [default: "backend"]},
                 %{name: :prompt_date, type: :string, opts: [default: {:today, :iso8601}]}
               ]
             }
           ]
  end

  test "exposes cron trigger idempotency strategy" do
    module =
      compile_module("""
      defmodule WorkflowWithIdempotentCronTrigger do
        use SquidMesh.Workflow

        workflow do
          trigger :scheduled_digest do
            cron "0 9 * * *", timezone: "UTC", idempotency: :return_existing_run
          end

          step :load_invoice, WorkflowWithIdempotentCronTrigger.LoadInvoice
        end
      end
      """)

    assert [
             %{
               name: :scheduled_digest,
               type: :cron,
               config: %{
                 expression: "0 9 * * *",
                 timezone: "UTC",
                 idempotency: :return_existing_run
               }
             }
           ] = module.workflow_definition().triggers
  end

  test "fails when a cron trigger declares an invalid idempotency strategy" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidCronIdempotency do
        use SquidMesh.Workflow

        workflow do
          trigger :scheduled_digest do
            cron "0 9 * * *", timezone: "UTC", idempotency: :retry
          end

          step :load_invoice, WorkflowWithInvalidCronIdempotency.LoadInvoice
        end
      end
      """,
      "trigger :scheduled_digest defines invalid cron idempotency strategy :retry"
    )
  end

  test "supports declarative built-in steps" do
    module =
      compile_module("""
      defmodule WorkflowWithBuiltInSteps do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :wait_for_settlement, :wait, duration: 250
          step :log_delivery, :log, message: "delivery completed", level: :info

          transition :wait_for_settlement, on: :ok, to: :log_delivery
          transition :log_delivery, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().steps == [
             %{name: :wait_for_settlement, module: :wait, opts: [duration: 250]},
             %{
               name: :log_delivery,
               module: :log,
               opts: [message: "delivery completed", level: :info]
             }
           ]
  end

  test "fails when a built-in wait step is missing duration" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidWaitStep do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :wait_for_settlement, :wait
        end
      end
      """,
      "built-in step :wait_for_settlement requires a positive :duration option"
    )
  end

  test "fails when a built-in log step is missing message" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidLogStep do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :log_delivery, :log, level: :warning
        end
      end
      """,
      "built-in step :log_delivery requires a non-empty :message option"
    )
  end

  test "fails when a payload default does not match the declared field type" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidPayloadDefault do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()

            payload do
              field :max_attempts, :integer, default: "five"
            end
          end

          step :load_invoice, WorkflowWithInvalidPayloadDefault.LoadInvoice
        end
      end
      """,
      "payload field :max_attempts defines an invalid default for type :integer"
    )
  end

  test "fails when a dynamic payload default does not match the declared field type" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidDynamicPayloadDefault do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()

            payload do
              field :prompt_date, :integer, default: {:today, :iso8601}
            end
          end

          step :load_invoice, WorkflowWithInvalidDynamicPayloadDefault.LoadInvoice
        end
      end
      """,
      "payload field :prompt_date defines an invalid default for type :integer"
    )
  end

  test "fails when a transition declares an unsupported outcome" do
    assert_compile_error(
      """
      defmodule WorkflowWithUnsupportedTransitionOutcome do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithUnsupportedTransitionOutcome.LoadInvoice
          transition :load_invoice, on: :unexpected, to: :complete
        end
      end
      """,
      "transition from :load_invoice defines unsupported outcome :unexpected"
    )
  end

  test "supports transitions declared on :error outcomes" do
    module =
      compile_module("""
      defmodule WorkflowWithErrorTransition do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :check_gateway, WorkflowWithErrorTransition.CheckGateway
          step :notify_operator, WorkflowWithErrorTransition.NotifyOperator

          transition :check_gateway, on: :error, to: :notify_operator
          transition :notify_operator, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().transitions == [
             %{from: :check_gateway, on: :error, to: :notify_operator},
             %{from: :notify_operator, on: :ok, to: :complete}
           ]
  end

  test "supports first-class approval step declarations" do
    module =
      compile_module("""
      defmodule WorkflowWithApprovalStep do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          approval_step :wait_for_review, output: :approval
          step :record_approval, WorkflowWithApprovalStep.RecordApproval
          step :record_rejection, WorkflowWithApprovalStep.RecordRejection

          transition :wait_for_review, on: :ok, to: :record_approval
          transition :wait_for_review, on: :error, to: :record_rejection
          transition :record_approval, on: :ok, to: :complete
          transition :record_rejection, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().steps == [
             %{name: :wait_for_review, module: :approval, opts: [output: :approval]},
             %{
               name: :record_approval,
               module: Module.safe_concat(module, RecordApproval),
               opts: []
             },
             %{
               name: :record_rejection,
               module: Module.safe_concat(module, RecordRejection),
               opts: []
             }
           ]
  end

  test "rejects built-in :pause steps in dependency-based workflows" do
    assert_compile_error(
      """
      defmodule DependencyWorkflowWithPause do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_account, DependencyWorkflowWithPause.LoadAccount
          step :wait_for_approval, :pause, after: [:load_account]
        end
      end
      """,
      "dependency-based workflows cannot declare built-in :pause steps"
    )
  end

  test "rejects approval steps in dependency-based workflows" do
    assert_compile_error(
      """
      defmodule DependencyWorkflowWithApproval do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_account, DependencyWorkflowWithApproval.LoadAccount
          approval_step :wait_for_review, after: [:load_account]
        end
      end
      """,
      "dependency-based workflows cannot declare built-in :approval steps"
    )
  end

  test "requires approval steps to declare both :ok and :error transitions" do
    assert_compile_error(
      """
      defmodule WorkflowWithIncompleteApprovalRouting do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          approval_step :wait_for_review
          step :record_approval, WorkflowWithIncompleteApprovalRouting.RecordApproval

          transition :wait_for_review, on: :ok, to: :record_approval
          transition :record_approval, on: :ok, to: :complete
        end
      end
      """,
      "approval step :wait_for_review must define both :ok and :error transitions"
    )
  end

  test "fails when duplicate transitions are declared for the same outcome" do
    assert_compile_error(
      """
      defmodule WorkflowWithDuplicateTransitions do
        use SquidMesh.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :check_gateway, WorkflowWithDuplicateTransitions.CheckGateway
          step :notify_operator, WorkflowWithDuplicateTransitions.NotifyOperator
          step :record_failure, WorkflowWithDuplicateTransitions.RecordFailure

          transition :check_gateway, on: :error, to: :notify_operator
          transition :check_gateway, on: :error, to: :record_failure
        end
      end
      """,
      "duplicate transition declared from :check_gateway on outcome :error"
    )
  end

  defp assert_compile_error(source, message) do
    error =
      assert_raise CompileError, fn ->
        Code.compile_string(source, "test/support/invalid_workflow.exs")
      end

    assert String.contains?(Exception.message(error), message)
  end

  defp compile_module(source) do
    [{module, _bytecode}] = Code.compile_string(source, "test/support/valid_workflow.exs")
    module
  end

  defp compiled_module!(compiled_modules, module) do
    compiled_module =
      Enum.find_value(compiled_modules, fn
        {^module, _bytecode} -> module
        _other -> nil
      end)

    case compiled_module do
      nil -> flunk("expected #{inspect(module)} to compile")
      module -> module
    end
  end

  defmodule InvoiceReminder do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_invoice, InvoiceReminder.LoadInvoice
      step :send_email, InvoiceReminder.SendEmail, retry: [max_attempts: 3]
      step :record_delivery, InvoiceReminder.RecordDelivery

      transition :load_invoice, on: :ok, to: :send_email
      transition :send_email, on: :ok, to: :record_delivery
      transition :record_delivery, on: :ok, to: :complete
    end
  end

  defmodule DependencyWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, DependencyWorkflow.LoadAccount
      step :load_invoice, DependencyWorkflow.LoadInvoice
      step :send_email, DependencyWorkflow.SendEmail, after: [:load_account, :load_invoice]
    end
  end
end
