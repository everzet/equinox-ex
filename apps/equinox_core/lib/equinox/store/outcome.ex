defmodule Equinox.Store.Outcome do
  alias Equinox.Events.{DomainEvent, EventData}
  alias Equinox.{Store, Codec}

  defstruct [:events, :context]
  @type t :: %__MODULE__{events: Enumerable.t(DomainEvent.t()), context: Store.sync_context()}
  @type sync_context :: map()

  @spec new(Enumerable.t(DomainEvent.t()), Store.sync_context()) :: t()
  def new(events, context \\ %{}), do: %__MODULE__{events: events, context: context}

  @spec produce_messages(t(), Codec.t()) :: Enumerable.t(EventData.t())
  def produce_messages(%__MODULE__{} = outcome, codec) do
    Enum.map(outcome.events, &codec.encode(&1, outcome.context))
  end
end
