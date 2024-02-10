defmodule Equinox.Cache.ProcessDictTest do
  use ExUnit.Case, async: true

  alias Equinox.Codec.StreamName
  alias Equinox.{Cache, Cache.ProcessDict, Store.State}

  @stream1 StreamName.decode!("Invoice-1", 1)
  @stream2 StreamName.decode!("Invoice-2", 1)

  test "returns nil if there is no value stored in the current process dictionary" do
    cache = ProcessDict.new()
    assert Cache.get(cache, @stream1, :infinity) == nil
  end

  test "returns cache if there is a fresh value" do
    cache = ProcessDict.new()

    Cache.put(cache, @stream1, State.new("A", 10))

    assert %State{value: "A"} = Cache.get(cache, @stream1, :infinity)
  end

  test "returns nil if cached value stream does not match provided one" do
    cache = ProcessDict.new()

    Cache.put(cache, @stream1, State.new("A", 10))

    assert Cache.get(cache, @stream2, :infinity) == nil
  end

  test "values older than given max_age are not returned" do
    cache = ProcessDict.new()

    Cache.put(cache, @stream1, State.new("A", 10))
    Process.sleep(5)

    assert Cache.get(cache, @stream1, 10).value == "A"
    assert Cache.get(cache, @stream1, 5) == nil
  end

  test "specifying max_age = 0 makes it so that no cache value is ever returned" do
    cache = ProcessDict.new()

    Cache.put(cache, @stream1, State.new("A", 10))

    assert Cache.get(cache, @stream1, 0) == nil
  end
end
