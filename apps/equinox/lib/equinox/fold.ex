defmodule Equinox.Fold do
  @callback initial() :: any()
  @callback fold(domain_events :: list(any()), state :: any()) :: any()
end
