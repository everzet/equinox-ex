defmodule Equinox.Lifetime.StopAfter30sOfInactivity do
  @behaviour Equinox.Lifetime

  def after_init(_), do: :timer.seconds(30)
  def after_query(_), do: :timer.seconds(30)
  def after_transact(_), do: :timer.seconds(30)
end
