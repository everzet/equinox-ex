defmodule Equinox.Fold do
  alias Equinox.{State, Telemetry}
  alias Equinox.Events.DomainEvent
  alias Equinox.Fold.Errors

  @type t :: module()

  @callback initial() :: State.value()
  @callback evolve(State.value(), DomainEvent.t()) :: State.value()

  @spec fold(Enumerable.t(DomainEvent.with_position()), State.t(), t()) :: State.t()
  def fold(domain_events, %State{} = state, fold) do
    Telemetry.span_fold(fold, state, fn ->
      Enum.reduce(domain_events, state, fn {event, position}, state ->
        try do
          State.update(state, &fold.evolve(&1, event), position)
        rescue
          exception ->
            reraise Errors.FoldError,
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
