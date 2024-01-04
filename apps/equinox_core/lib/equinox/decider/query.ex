defmodule Equinox.Decider.Query do
  alias Equinox.State

  @type t :: (State.value() -> any())

  @spec execute(t(), State.t()) :: any()
  def execute(query, %State{value: value}), do: query.(value)
end
