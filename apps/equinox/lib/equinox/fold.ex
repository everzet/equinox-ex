defmodule Equinox.Fold do
  alias Equinox.Events.DomainEvent

  @type t :: module()
  @type state :: any()

  @callback initial() :: state()
  @callback fold(list(DomainEvent.t()), state()) :: state()
end
