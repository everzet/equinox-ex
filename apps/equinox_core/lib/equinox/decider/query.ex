defmodule Equinox.Decider.Query do
  alias Equinox.Fold

  @type t :: (Fold.result() -> any())

  @spec execute(t(), Fold.result()) :: any()
  def execute(query, state), do: query.(state)
end
