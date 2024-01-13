defmodule Equinox.Codec do
  alias Equinox.Events.{TimelineEvent, DomainEvent, EventData}

  @type t :: module()
  @type context :: map()

  @callback decode(TimelineEvent.t()) :: DomainEvent.t()
  @callback encode(DomainEvent.t(), context()) :: EventData.t()
end
