defmodule Equinox.Cache.LRUTest do
  use ExUnit.Case, async: false

  alias Equinox.Codec.StreamName
  alias Equinox.{Cache, Cache.LRU, Store.State}

  @stream1 StreamName.decode!("Invoice-1", 1)
  @stream2 StreamName.decode!("Invoice-2", 1)
  @stream3 StreamName.decode!("Invoice-3", 1)

  test "configured via helper function new/1" do
    assert LRU.new(name: TestCache) == %LRU{name: TestCache}
  end

  test "max_memory can be specified in bytes, kilobytes, megabytes and gigabytes" do
    start_supervised!({LRU, name: TestCache1, max_memory: 1_000_000_000})
    start_supervised!({LRU, name: TestCache2, max_memory: {1_000_000, :kb}})
    start_supervised!({LRU, name: TestCache3, max_memory: {1_000, :mb}})
    start_supervised!({LRU, name: TestCache4, max_memory: {1, :gb}})
  end

  test "returns nil if there is no value stored for given key" do
    start_supervised!({LRU, name: TestCache, max_size: 10})
    cache = LRU.new(name: TestCache)
    assert Cache.get(cache, @stream1, :infinity) == nil
  end

  test "returns cache if there is a fresh value" do
    start_supervised!({LRU, name: TestCache, max_size: 10})
    cache = LRU.new(name: TestCache)

    Cache.put(cache, @stream1, State.new("A", 10))

    assert %State{value: "A"} = Cache.get(cache, @stream1, :infinity)
  end

  test "values older than given max_age are not returned" do
    start_supervised!({LRU, name: TestCache, max_size: 1})
    cache = LRU.new(name: TestCache)

    Cache.put(cache, @stream1, State.new("A", 10))
    Process.sleep(5)

    assert Cache.get(cache, @stream1, 10).value == "A"
    assert Cache.get(cache, @stream1, 5) == nil
  end

  test "specifying max_age = 0 makes it so that no cache value is ever returned" do
    start_supervised!({LRU, name: TestCache, max_size: 1})
    cache = LRU.new(name: TestCache)

    Cache.put(cache, @stream1, State.new("A", 10))

    assert Cache.get(cache, @stream1, 0) == nil
  end

  test "oldest value is evicted from cache when max_size is reached" do
    start_supervised!({LRU, name: TestCache, max_size: 2})
    cache = LRU.new(name: TestCache)

    Cache.put(cache, @stream1, State.new("A", 10))
    Cache.put(cache, @stream2, State.new("B", 10))
    Cache.put(cache, @stream3, State.new("C", 10))

    assert Cache.get(cache, @stream1, :infinity) == nil
    assert Cache.get(cache, @stream2, :infinity).value == "B"
    assert Cache.get(cache, @stream3, :infinity).value == "C"
  end

  test "oldest values are evicted from cache when max_memory is reached" do
    start_supervised!({LRU, name: TestCache, max_memory: {100, :kb}})
    cache = LRU.new(name: TestCache)

    gen_kbs_of_data = fn kbs ->
      data = {:one, :two}

      # `data` is 4 words in size - 2 words for tuple itself + 1 word per each atom
      # https://www.erlang.org/doc/efficiency_guide/advanced.html#memory
      data_size = 4
      # how many bytes is each word in the current environment?
      word_size = :erlang.system_info(:wordsize)

      # how many pieces of `data` will fully fill 1 kilobyte (1k bytes)?
      data_per_kb = round(1_000 / (data_size * word_size))

      for(_ <- 1..(data_per_kb * kbs), do: data)
    end

    Cache.put(cache, @stream1, State.new(gen_kbs_of_data.(25), 10))
    Cache.put(cache, @stream2, State.new(gen_kbs_of_data.(25), 10))
    Cache.put(cache, @stream3, State.new(gen_kbs_of_data.(70), 10))

    assert Cache.get(cache, @stream1, :infinity) == nil
    assert Cache.get(cache, @stream2, :infinity) == nil
    assert Cache.get(cache, @stream3, :infinity).value
  end

  test "all values will be continuously evicted if max_memory is set too low" do
    start_supervised!({LRU, name: TestCache, max_memory: 10})
    cache = LRU.new(name: TestCache)

    Cache.put(cache, @stream1, State.new("A", 10))
    Cache.put(cache, @stream2, State.new("B", 10))

    assert Cache.get(cache, @stream1, :infinity) == nil
    assert Cache.get(cache, @stream2, :infinity) == nil
  end

  test "putting value ensures it is not evicted next" do
    start_supervised!({LRU, name: TestCache, max_size: 2})
    cache = LRU.new(name: TestCache)

    Cache.put(cache, @stream1, State.new("A", 10))
    Cache.put(cache, @stream2, State.new("B", 10))
    Cache.put(cache, @stream1, State.new("A", 11))
    Cache.put(cache, @stream3, State.new("C", 10))

    assert Cache.get(cache, @stream1, :infinity).value == "A"
    assert Cache.get(cache, @stream2, :infinity) == nil
    assert Cache.get(cache, @stream3, :infinity).value == "C"
  end

  test "getting value ensures it is not evicted next" do
    start_supervised!({LRU, name: TestCache, max_size: 2})
    cache = LRU.new(name: TestCache)

    Cache.put(cache, @stream1, State.new("A", 10))
    Cache.put(cache, @stream2, State.new("B", 10))
    Cache.get(cache, @stream1, :infinity)
    Cache.put(cache, @stream3, State.new("C", 10))

    assert Cache.get(cache, @stream1, :infinity).value == "A"
    assert Cache.get(cache, @stream2, :infinity) == nil
    assert Cache.get(cache, @stream3, :infinity).value == "C"
  end
end
