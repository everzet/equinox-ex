defmodule Equinox.CacheMock.Config do
  defstruct mod: Equinox.CacheMock

  @callback fetch(any(), any()) :: any()
  @callback insert(any(), any()) :: any()

  defimpl Equinox.Cache do
    def fetch(cache, stream, max_age), do: cache.mod.fetch(stream, max_age)
    def insert(cache, stream, state), do: cache.mod.insert(stream, state)
  end
end

Mox.defmock(Equinox.CacheMock, for: Equinox.CacheMock.Config)
