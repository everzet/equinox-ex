defmodule Equinox.Store.EventsToSync do
  alias Equinox.{Codec, Events.DomainEvent, Events.EventData}

  @enforce_keys [:events, :context]
  defstruct [:events, :context]

  @type t :: %__MODULE__{events: list(DomainEvent.t()), context: context()}
  @type events :: nil | DomainEvent.t() | list(DomainEvent.t())
  @type context :: map()

  @spec new(events(), context()) :: t()
  def new(events, context \\ %{}), do: %__MODULE__{events: List.wrap(events), context: context}

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{events: []}), do: true
  def empty?(%__MODULE__{}), do: false

  @spec encode(t(), Codec.t(), DomainEvent.serialize()) :: list(EventData.t())
  def encode(%__MODULE__{} = to_sync, codec, serialize) do
    Enum.map(to_sync.events, &DomainEvent.encode(&1, codec, to_sync.context, serialize))
  end
end
