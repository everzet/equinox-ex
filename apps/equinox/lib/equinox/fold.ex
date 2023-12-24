defmodule Equinox.Fold do
  alias Equinox.Events.DomainEvent
  alias Equinox.State

  @type t :: module()

  @callback initial() :: State.value()
  @callback evolve(State.value(), DomainEvent.t()) :: State.value()

  defmodule FoldError do
    @enforce_keys [:message]
    defexception [:message, :exception]
    @type t :: %__MODULE__{message: String.t(), exception: nil | Exception.t()}
  end

  @spec fold(Enumerable.t(DomainEvent.indexed()), State.t(), t()) :: State.t()
  def fold(domain_events, state, fold) do
    Enum.reduce(domain_events, state, fn {event, position}, state ->
      try do
        %State{value: fold.evolve(state.value, event), version: position}
      rescue
        exception ->
          reraise FoldError,
                  [
                    message: "#{inspect(fold)}.evolve: #{inspect(exception)}",
                    exception: exception
                  ],
                  __STACKTRACE__
      end
    end)
  end
end
