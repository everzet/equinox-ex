defmodule Equinox.StoreMock.Config do
  defstruct [:allow_from, mod: Equinox.StoreMock]

  @callback load(any(), any()) :: any()
  @callback sync(any(), any(), any()) :: any()

  defimpl Equinox.Store do
    def load(store, stream, policy), do: store.mod.load(stream, policy)
    def sync(store, stream, state, to_sync), do: store.mod.sync(stream, state, to_sync)
  end
end

Mox.defmock(Equinox.StoreMock, for: Equinox.StoreMock.Config)
