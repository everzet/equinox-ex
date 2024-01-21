defmodule Equinox.Store.EventsToSync do
  alias Equinox.Events.{DomainEvent, EventData}
  alias Equinox.Codec

  defstruct [:events, :context]
  @type t :: %__MODULE__{events: Enumerable.t(DomainEvent.t()), context: context()}
  @type context :: map()

  @spec new(nil | DomainEvent.t() | list(DomainEvent.t()), context()) :: t()
  def new(events, context \\ %{}), do: %__MODULE__{events: List.wrap(events), context: context}

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{events: []}), do: true
  def empty?(%__MODULE__{}), do: false

  @spec to_messages(t(), Codec.t()) :: Enumerable.t(EventData.t())
  def to_messages(%__MODULE__{} = to_sync, codec) do
    Enum.map(to_sync.events, &codec.encode(&1, to_sync.context))
  end
end
