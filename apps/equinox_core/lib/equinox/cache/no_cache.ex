defmodule Equinox.Cache.NoCache do
  defstruct []

  def config, do: %__MODULE__{}

  defimpl Equinox.Cache do
    @impl Equinox.Cache
    def fetch(_cache, _stream_name, _max_age), do: nil

    @impl Equinox.Cache
    def insert(_cache, _stream_name, _state), do: :ok
  end
end
