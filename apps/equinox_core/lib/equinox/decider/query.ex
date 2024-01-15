defmodule Equinox.Decider.Query do
  alias Equinox.Fold

  @type t :: (Fold.result() -> term())

  @spec execute(t(), Fold.result()) :: term()
  def execute(query, state), do: query.(state)
end
