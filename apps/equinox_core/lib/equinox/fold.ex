defmodule Equinox.Fold do
  alias Equinox.{State, Telemetry}
  alias Equinox.Events.DomainEvent

  @type t :: module()

  @callback initial() :: State.value()
  @callback evolve(State.value(), DomainEvent.t()) :: State.value()

  defmodule FoldError do
    @enforce_keys [:message]
    defexception [:message, :exception]
    @type t :: %__MODULE__{message: String.t(), exception: nil | Exception.t()}
  end

  @spec fold(Enumerable.t(DomainEvent.with_position()), State.t(), t()) :: State.t()
  def fold(domain_events, %State{} = state, fold) do
    Telemetry.span_fold(fold, state, fn ->
      Enum.reduce(domain_events, state, fn {event, position}, state ->
        try do
          State.update(state, &fold.evolve(&1, event), position)
        rescue
          exception ->
            reraise FoldError,
                    [
                      message: "#{inspect(fold)}.evolve: #{Exception.message(exception)}",
                      exception: exception
                    ],
                    __STACKTRACE__
        end
      end)
    end)
  end
end
