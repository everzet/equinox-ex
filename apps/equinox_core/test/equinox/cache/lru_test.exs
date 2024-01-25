defmodule Equinox.Cache.LRUTest do
  use ExUnit.Case, async: false

  alias Equinox.Codec.StreamName
  alias Equinox.{Cache, Cache.LRU, Store.State}

  @stream1 StreamName.decode!("Invoice-1")
  @stream2 StreamName.decode!("Invoice-2")
  @stream3 StreamName.decode!("Invoice-3")

  test "returns nothing if there is no cache" do
    start_supervised!({LRU, name: TestCache, max_size: 10, max_memory: 100_000})
    cache = LRU.new(name: TestCache)
    assert Cache.get(cache, @stream1, :infinity) == nil
  end

  test "returns cache if there is fresh one" do
    start_supervised!({LRU, name: TestCache, max_size: 10, max_memory: 100_000})
    cache = LRU.new(name: TestCache)
    Cache.put(cache, @stream1, State.new(:val, 10))
    assert %State{value: :val} = Cache.get(cache, @stream1, :infinity)
  end

  test "cache older than required max_age is not returned" do
    start_supervised!({LRU, name: TestCache, max_size: 1, max_memory: 100_000})
    cache = LRU.new(name: TestCache)

    Cache.put(cache, @stream1, State.new(:val1, 10))
    Process.sleep(5)

    assert Cache.get(cache, @stream1, 5) == nil
    assert Cache.get(cache, @stream1, 10) != nil
  end

  test "specifying max_age = 0 means cache is never returned" do
    start_supervised!({LRU, name: TestCache, max_size: 1, max_memory: 100_000})
    cache = LRU.new(name: TestCache)
    Cache.put(cache, @stream1, State.new(:val1, 10))
    assert Cache.get(cache, @stream1, 0) == nil
  end

  test "evicts oldest cache value if it breaks the size limit" do
    start_supervised!({LRU, name: TestCache, max_size: 2, max_memory: 100_000})
    cache = LRU.new(name: TestCache)

    Cache.put(cache, @stream1, State.new(:val1, 10))
    Cache.put(cache, @stream2, State.new(:val2, 10))
    Cache.put(cache, @stream3, State.new(:val3, 10))

    assert Cache.get(cache, @stream1, :infinity) == nil
    assert %State{value: :val2} = Cache.get(cache, @stream2, :infinity)
    assert %State{value: :val3} = Cache.get(cache, @stream3, :infinity)
  end

  test "re-putting value puts it at the bottom of eviction list" do
    start_supervised!({LRU, name: TestCache, max_size: 2, max_memory: 100_000})
    cache = LRU.new(name: TestCache)

    Cache.put(cache, @stream1, State.new(:val1, 10))
    Cache.put(cache, @stream2, State.new(:val2, 10))
    Cache.put(cache, @stream1, State.new(:val12, 11))
    Cache.put(cache, @stream3, State.new(:val3, 10))

    assert Cache.get(cache, @stream2, :infinity) == nil
    assert %State{value: :val12} = Cache.get(cache, @stream1, :infinity)
    assert %State{value: :val3} = Cache.get(cache, @stream3, :infinity)
  end

  test "getting value puts it at the bottom of eviction list" do
    start_supervised!({LRU, name: TestCache, max_size: 2, max_memory: 100_000})
    cache = LRU.new(name: TestCache)

    Cache.put(cache, @stream1, State.new(:val1, 10))
    Cache.put(cache, @stream2, State.new(:val2, 10))
    Cache.get(cache, @stream1, :infinity)
    Cache.put(cache, @stream3, State.new(:val3, 10))

    assert Cache.get(cache, @stream2, :infinity) == nil
    assert %State{value: :val1} = Cache.get(cache, @stream1, :infinity)
    assert %State{value: :val3} = Cache.get(cache, @stream3, :infinity)
  end
end
