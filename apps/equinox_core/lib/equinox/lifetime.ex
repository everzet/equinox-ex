defmodule Equinox.Lifetime do
  alias Equinox.Fold

  @type t :: module()

  @callback after_init(Fold.result()) :: timeout()
  @callback after_query(Fold.result()) :: timeout()
  @callback after_transact(Fold.result()) :: timeout()
end
