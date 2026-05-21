defmodule SquidMesh.Runtime.StepExecutor.Progression do
  @moduledoc false

  alias SquidMesh.Runs

  @type attrs_fun :: Runs.Store.attrs_fun()
  @type dispatch_error_handler :: (SquidMesh.Run.t(), term() -> Runs.Store.transition_attrs())

  defmodule Complete do
    @moduledoc false
    @enforce_keys [:attrs_fun]
    defstruct [:attrs_fun]
    @type t :: %__MODULE__{attrs_fun: SquidMesh.Runtime.StepExecutor.Progression.attrs_fun()}
  end

  defmodule Update do
    @moduledoc false
    @enforce_keys [:attrs_fun]
    defstruct [:attrs_fun]
    @type t :: %__MODULE__{attrs_fun: SquidMesh.Runtime.StepExecutor.Progression.attrs_fun()}
  end

  defmodule DispatchRun do
    @moduledoc false
    @enforce_keys [:attrs_fun, :dispatch_opts, :dispatch_error_handler]
    defstruct [:attrs_fun, :dispatch_opts, :dispatch_error_handler]

    @type t :: %__MODULE__{
            attrs_fun: SquidMesh.Runtime.StepExecutor.Progression.attrs_fun(),
            dispatch_opts: keyword(),
            dispatch_error_handler:
              SquidMesh.Runtime.StepExecutor.Progression.dispatch_error_handler()
          }
  end

  defmodule DispatchSteps do
    @moduledoc false
    @enforce_keys [:attrs_fun, :steps, :dispatch_opts, :dispatch_error_handler]
    defstruct [:attrs_fun, :steps, :dispatch_opts, :dispatch_error_handler]

    @type t :: %__MODULE__{
            attrs_fun: SquidMesh.Runtime.StepExecutor.Progression.attrs_fun(),
            steps: [atom()],
            dispatch_opts: keyword(),
            dispatch_error_handler:
              SquidMesh.Runtime.StepExecutor.Progression.dispatch_error_handler()
          }
  end

  @type t :: Complete.t() | Update.t() | DispatchRun.t() | DispatchSteps.t()

  @doc false
  @spec complete(attrs_fun()) :: Complete.t()
  def complete(attrs_fun), do: %Complete{attrs_fun: attrs_fun}

  @doc false
  @spec update(attrs_fun()) :: Update.t()
  def update(attrs_fun), do: %Update{attrs_fun: attrs_fun}

  @doc false
  @spec dispatch_run(attrs_fun(), keyword(), dispatch_error_handler()) :: DispatchRun.t()
  def dispatch_run(attrs_fun, dispatch_opts, dispatch_error_handler) do
    %DispatchRun{
      attrs_fun: attrs_fun,
      dispatch_opts: dispatch_opts,
      dispatch_error_handler: dispatch_error_handler
    }
  end

  @doc false
  @spec dispatch_steps(attrs_fun(), [atom()], keyword(), dispatch_error_handler()) ::
          DispatchSteps.t()
  def dispatch_steps(attrs_fun, steps, dispatch_opts, dispatch_error_handler) do
    %DispatchSteps{
      attrs_fun: attrs_fun,
      steps: steps,
      dispatch_opts: dispatch_opts,
      dispatch_error_handler: dispatch_error_handler
    }
  end
end
