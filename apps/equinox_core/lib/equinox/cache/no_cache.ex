defmodule Equinox.Cache.NoCache do
  defstruct []

  def new(_ \\ nil), do: %__MODULE__{}

  defimpl Equinox.Cache do
    @impl Equinox.Cache
    def get(_cache, _stream_name, _max_age), do: nil

    @impl Equinox.Cache
    def put(_cache, _stream_name, _state), do: :ok
  end
end
