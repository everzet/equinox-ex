defmodule Equinox.Fold do
  alias Equinox.Events.DomainEvent

  @type t :: module()
  @type state :: any()

  @callback initial() :: state()
  @callback evolve(state(), DomainEvent.t()) :: state()

  defmodule FoldError do
    defexception [:message, :exception]
    @type t :: %__MODULE__{message: String.t(), exception: nil | Exception.t()}
  end
end
