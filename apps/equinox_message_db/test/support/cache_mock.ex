defmodule Equinox.CacheMock.Config do
  defstruct mod: Equinox.CacheMock

  @callback get(any(), any()) :: any()
  @callback put(any(), any()) :: any()

  defimpl Equinox.Cache do
    def get(cache, stream, max_age), do: cache.mod.get(stream, max_age)
    def put(cache, stream, state), do: cache.mod.put(stream, state)
  end
end

Mox.defmock(Equinox.CacheMock, for: Equinox.CacheMock.Config)
