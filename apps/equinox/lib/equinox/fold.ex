defmodule Equinox.Fold do
  @type t :: module()

  @callback initial() :: any()
  @callback fold(domain_events :: list(any()), state :: any()) :: any()
end
