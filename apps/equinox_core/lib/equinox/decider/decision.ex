defmodule Equinox.Decider.Decision do
  alias Equinox.State
  alias Equinox.Events.DomainEvent

  @type t ::
          (State.value() ->
             nil
             | DomainEvent.t()
             | list(DomainEvent.t())
             | {:ok, DomainEvent.t() | list(DomainEvent.t())}
             | {:error, term()})

  @spec execute(t(), State.t()) :: {:ok, list(DomainEvent.t())} | {:error, term()}
  def execute(decision, %State{value: value}) do
    case decision.(value) do
      {:error, error} -> {:error, error}
      {:ok, event_or_events} -> {:ok, List.wrap(event_or_events)}
      nil_or_event_or_events -> {:ok, List.wrap(nil_or_event_or_events)}
    end
  end
end
