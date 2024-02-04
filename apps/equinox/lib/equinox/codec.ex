defmodule Equinox.Codec do
  alias Equinox.Events.{DomainEvent, EventData, TimelineEvent}

  @type t :: module()
  @type context :: map()

  @callback decode(TimelineEvent.t()) :: nil | DomainEvent.t()
  @callback encode(DomainEvent.t(), context()) :: EventData.t()
end
