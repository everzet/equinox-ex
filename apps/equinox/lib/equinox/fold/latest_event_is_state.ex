defmodule Equinox.Fold.LatestEventIsState do
  @behaviour Equinox.Fold

  @impl Equinox.Fold
  def initial, do: nil

  @impl Equinox.Fold
  def fold(events, _), do: Enum.at(events, -1)
end
