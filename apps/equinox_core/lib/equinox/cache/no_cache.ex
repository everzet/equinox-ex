defmodule Equinox.Cache.NoCache do
  @behaviour Equinox.Cache

  def fetch(_stream_name, _max_age), do: nil
  def insert(_stream_name, _state), do: :ok
end
