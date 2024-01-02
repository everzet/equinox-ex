defmodule Equinox.Fold do
  alias Equinox.{State, Telemetry}
  alias Equinox.Events.DomainEvent
  alias Equinox.Fold.Errors

  @type t :: module()

  @callback initial() :: State.value()
  @callback evolve(State.value(), DomainEvent.t()) :: State.value()

  @spec evolve(DomainEvent.with_position(), State.t(), t()) :: State.t()
  def evolve({event, position} = domain_event, %State{} = state, fold) do
    Telemetry.span_fold_evolve(fold, domain_event, state, fn ->
      State.update(state, &fold.evolve(&1, event), position)
    end)
  rescue
    exception ->
      reraise Errors.EvolveError,
              [
                message: "#{inspect(fold)}: #{Exception.message(exception)}",
                exception: exception
              ],
              __STACKTRACE__
  end

  @spec fold(Enumerable.t(DomainEvent.with_position()), State.t(), t()) :: State.t()
  def fold(domain_events, %State{} = state, fold) do
    Telemetry.span_fold(fold, domain_events, state, fn ->
      Enum.reduce(domain_events, state, &evolve(&1, &2, fold))
    end)
  end
end
