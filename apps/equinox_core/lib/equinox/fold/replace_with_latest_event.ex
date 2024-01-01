defmodule Equinox.Fold.ReplaceWithLatestEvent do
  @behaviour Equinox.Fold

  @impl Equinox.Fold
  def initial(), do: nil

  @impl Equinox.Fold
  def evolve(_, event), do: event
end
