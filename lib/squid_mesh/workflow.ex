defmodule SquidMesh.Workflow do
  @moduledoc """
  Declarative workflow contract for Squid Mesh workflow modules.

  ## Example

      defmodule Billing.InvoiceReminder do
        use SquidMesh.Workflow

        workflow do
          trigger :invoice_delivery do
            manual()

            payload do
              field :account_id, :string
              field :invoice_id, :string
            end
          end

          step :load_invoice, Billing.Steps.LoadInvoice
          step :send_email, Billing.Steps.SendReminderEmail, retry: [max_attempts: 3]

          transition :load_invoice, on: :ok, to: :send_email
        end
      end

  The contract defined here captures workflow structure. Validation and runtime
  execution behavior are added in subsequent slices.
  """

  @contract %{
    required: [:trigger, :step],
    optional: [:transition]
  }

  alias SquidMesh.Workflow.StepSpec
  alias SquidMesh.Workflow.Validation

  @doc """
  Injects the workflow DSL and compile-time collectors into a workflow module.
  """
  @spec __using__(keyword()) :: Macro.t()
  defmacro __using__(_opts) do
    quote do
      use SquidMesh.Workflow.Dsl

      import SquidMesh.Workflow,
        only: [
          trigger: 2,
          manual: 0,
          cron: 1,
          cron: 2,
          payload: 1,
          field: 2,
          field: 3,
          transition: 2
        ]

      Module.register_attribute(__MODULE__, :squid_mesh_triggers, accumulate: true)
      Module.register_attribute(__MODULE__, :squid_mesh_transitions, accumulate: true)
      Module.register_attribute(__MODULE__, :squid_mesh_current_field_target, persist: false)
      Module.register_attribute(__MODULE__, :squid_mesh_current_trigger_name, persist: false)

      Module.register_attribute(
        __MODULE__,
        :squid_mesh_current_trigger_definitions,
        persist: false
      )

      Module.register_attribute(
        __MODULE__,
        :squid_mesh_current_trigger_payload_fields,
        persist: false
      )

      @before_compile SquidMesh.Workflow
    end
  end

  @doc """
  Declares a named workflow trigger.
  """
  defmacro trigger(name, do: block) do
    quote bind_quoted: [name: name, block: Macro.escape(block)] do
      Module.put_attribute(__MODULE__, :squid_mesh_current_trigger_name, name)
      Module.put_attribute(__MODULE__, :squid_mesh_current_trigger_definitions, [])
      Module.put_attribute(__MODULE__, :squid_mesh_current_trigger_payload_fields, [])

      Code.eval_quoted(block, [], __ENV__)

      trigger = %{
        name: Module.get_attribute(__MODULE__, :squid_mesh_current_trigger_name),
        definitions:
          __MODULE__
          |> Module.get_attribute(:squid_mesh_current_trigger_definitions)
          |> Enum.reverse(),
        payload:
          __MODULE__
          |> Module.get_attribute(:squid_mesh_current_trigger_payload_fields)
          |> Enum.reverse()
      }

      @squid_mesh_triggers trigger

      Module.delete_attribute(__MODULE__, :squid_mesh_current_trigger_name)
      Module.delete_attribute(__MODULE__, :squid_mesh_current_trigger_definitions)
      Module.delete_attribute(__MODULE__, :squid_mesh_current_trigger_payload_fields)
    end
  end

  @doc """
  Declares a manual trigger.
  """
  defmacro manual do
    quote do
      SquidMesh.Workflow.__push_current_trigger_definition__(__MODULE__, %{
        type: :manual,
        config: %{}
      })
    end
  end

  @doc """
  Declares a cron-based trigger.

  Pass `idempotency: :return_existing_run` when duplicate deliveries should
  return the first run, or `idempotency: :skip_duplicate` when duplicates should
  be reported as skipped. Idempotent cron triggers require either a
  scheduler-provided `:signal_id` or an `:intended_window` with both bounds so
  the runtime can derive a stable idempotency key.
  """
  defmacro cron(expression, opts \\ []) do
    quote bind_quoted: [expression: expression, opts: opts] do
      base_config = %{
        expression: expression,
        timezone: Keyword.get(opts, :timezone)
      }

      config =
        if Keyword.has_key?(opts, :idempotency) do
          Map.put(base_config, :idempotency, Keyword.get(opts, :idempotency))
        else
          base_config
        end

      SquidMesh.Workflow.__push_current_trigger_definition__(__MODULE__, %{
        type: :cron,
        config: config
      })
    end
  end

  @doc """
  Declares the payload contract for the current trigger.
  """
  defmacro payload(do: block) do
    quote bind_quoted: [block: Macro.escape(block)] do
      Module.put_attribute(__MODULE__, :squid_mesh_current_field_target, :trigger_payload)
      Code.eval_quoted(block, [], __ENV__)
      Module.delete_attribute(__MODULE__, :squid_mesh_current_field_target)
    end
  end

  @doc """
  Declares one payload field inside a trigger payload block.
  """
  defmacro field(name, type, opts \\ []) do
    quote bind_quoted: [name: name, type: type, opts: opts] do
      field = %{name: name, type: type, opts: opts}

      case Module.get_attribute(__MODULE__, :squid_mesh_current_field_target) do
        :trigger_payload ->
          SquidMesh.Workflow.__push_current_trigger_payload_field__(__MODULE__, field)

        _other ->
          raise CompileError,
            file: __ENV__.file,
            line: __ENV__.line,
            description: "field/3 must be declared inside a trigger payload block"
      end
    end
  end

  @doc """
  Declares a transition from one step outcome to the next step.
  """
  defmacro transition(from, opts) do
    quote bind_quoted: [from: from, opts: opts] do
      transition = %{
        from: from,
        on: Keyword.fetch!(opts, :on),
        to: Keyword.fetch!(opts, :to)
      }

      transition_config =
        case Keyword.fetch(opts, :recovery) do
          {:ok, recovery} -> Map.put(transition, :recovery, recovery)
          :error -> transition
        end

      @squid_mesh_transitions transition_config
    end
  end

  @doc """
  Validates the collected workflow declarations and emits runtime accessors.
  """
  @spec __before_compile__(Macro.Env.t()) :: Macro.t()
  defmacro __before_compile__(env) do
    env
    |> build_definition!()
    |> quoted_definition()
  end

  defp quoted_definition(definition) do
    quote do
      @doc false
      def workflow_definition do
        SquidMesh.Workflow.__resolve_runtime_definition__(unquote(Macro.escape(definition)))
      end

      @doc false
      def __workflow__(:definition), do: workflow_definition()

      @doc false
      def __workflow__(:contract), do: unquote(Macro.escape(@contract))

      @doc false
      def __workflow__(:steps), do: workflow_definition().steps

      @doc false
      def __workflow__(key)
          when key in [
                 :entry_step,
                 :entry_steps,
                 :initial_step,
                 :payload,
                 :retries,
                 :transitions,
                 :triggers
               ] do
        Map.fetch!(workflow_definition(), key)
      end
    end
  end

  defp build_definition!(env) do
    steps = spark_steps(env.module)

    definition = %{
      triggers: module_attribute(env.module, :squid_mesh_triggers),
      steps: steps,
      transitions: module_attribute(env.module, :squid_mesh_transitions),
      retries: Validation.derive_retries(steps)
    }

    Validation.validate!(definition, env)

    triggers = Validation.normalize_triggers!(definition)

    definition
    |> Map.put(:triggers, triggers)
    |> Map.put(:payload, Validation.workflow_payload!(triggers))
    |> Map.put(:entry_steps, Validation.entry_steps!(definition, env))
    |> Map.put(:initial_step, Validation.initial_step!(definition, env))
    |> Map.put(:entry_step, Validation.entry_step!(definition, env))
  end

  defp module_attribute(module, attribute) do
    module
    |> Module.get_attribute(attribute)
    |> Enum.reverse()
  end

  defp spark_steps(module) do
    module
    |> SquidMesh.Workflow.Info.steps()
    |> Enum.map(fn %StepSpec{} = step ->
      step
      |> Map.from_struct()
      |> Map.take([:name, :module, :opts, :metadata])
      |> maybe_drop_interop_metadata()
      |> Map.reject(fn {_key, value} -> value in [nil, %{}] end)
    end)
  end

  defp maybe_drop_interop_metadata(%{metadata: %{contract: :squid_mesh_step}} = step), do: step
  defp maybe_drop_interop_metadata(step), do: Map.delete(step, :metadata)

  @doc """
  Resolves late-bound runtime metadata for generated workflow definitions.

  Workflow modules store a compile-time definition, but step modules may expose
  Squid Mesh step metadata that should be read when the workflow definition is
  consumed. This keeps generated workflow modules small while preserving the
  runtime contract used by dispatch and inspection.
  """
  @spec __resolve_runtime_definition__(map()) :: map()
  def __resolve_runtime_definition__(definition) when is_map(definition) do
    Map.update!(definition, :steps, fn steps ->
      Enum.map(steps, &resolve_step_metadata/1)
    end)
  end

  defp resolve_step_metadata(%{module: module} = step) do
    case SquidMesh.Step.metadata(module) do
      %{} = metadata -> Map.put(step, :metadata, metadata)
      nil -> maybe_drop_interop_metadata(step)
    end
  end

  @doc """
  Adds one trigger definition to the workflow module currently being compiled.
  """
  @spec __push_current_trigger_definition__(module(), map()) :: :ok
  def __push_current_trigger_definition__(module, definition)
      when is_atom(module) and is_map(definition) do
    definitions = Module.get_attribute(module, :squid_mesh_current_trigger_definitions) || []

    Module.put_attribute(module, :squid_mesh_current_trigger_definitions, [
      definition | definitions
    ])

    :ok
  end

  @doc """
  Adds one payload field to the trigger currently being compiled.
  """
  @spec __push_current_trigger_payload_field__(module(), map()) :: :ok
  def __push_current_trigger_payload_field__(module, field)
      when is_atom(module) and is_map(field) do
    fields = Module.get_attribute(module, :squid_mesh_current_trigger_payload_fields) || []
    Module.put_attribute(module, :squid_mesh_current_trigger_payload_fields, [field | fields])
    :ok
  end
end
