defmodule Equinox.Fold do
  alias Equinox.Events.DomainEvent

  @type t :: module()
  @type result :: any()

  @callback initial() :: result()
  @callback fold(Enumerable.t(DomainEvent.t()), initial :: result()) :: result()
end
