defmodule Equinox.Cache.NoCacheTest do
  use ExUnit.Case, async: true

  alias Equinox.Codec.StreamName
  alias Equinox.{Cache, Cache.NoCache, Store.State}

  @stream StreamName.decode!("Invoice-1", 1)

  test "configured via helper function new/1" do
    assert NoCache.new() == %NoCache{}
  end

  test "never stores any cache" do
    cache = NoCache.new()
    assert :ok = Cache.put(cache, @stream, State.new("A", 10))
    refute Cache.get(cache, @stream, :infinity)
  end
end
