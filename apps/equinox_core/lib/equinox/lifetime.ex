defmodule Equinox.Lifetime do
  alias Equinox.State

  @type t :: module()
  @callback after_init(State.value()) :: timeout()
  @callback after_query(State.value()) :: timeout()
  @callback after_transact(State.value()) :: timeout()
end
