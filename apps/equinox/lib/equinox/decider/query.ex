defmodule Equinox.Decider.Query do
  alias Equinox.Fold

  @type result :: term()
  @type t :: (Fold.result() -> result())

  @spec execute(t(), Fold.result()) :: result()
  def execute(query, state), do: query.(state)
end
