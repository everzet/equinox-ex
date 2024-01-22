defmodule Equinox.Cache.LRU do
  defmodule Options do
    @opts NimbleOptions.new!(
            name: [
              type: :atom,
              required: true,
              doc: "Name of the cache process and ETS table"
            ],
            max_size: [
              type: {:or, [:pos_integer, {:in, [:infinity]}]},
              default: :infinity,
              doc:
                "Maximum number of stored items. Every time this number reached - the least touched item is evicted"
            ],
            max_memory: [
              type:
                {:or,
                 [
                   :pos_integer,
                   {:tuple, [:pos_integer, {:in, [:kb, :mb, :gb]}]},
                   {:in, [:infinity]}
                 ]},
              default: :infinity,
              doc:
                "Maximum total consumed memory in bytes. Every time this number reached - the least touched item is evicted"
            ]
          )

    @type t :: [o()]
    @type o :: unquote(NimbleOptions.option_typespec(@opts))

    def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
    def docs, do: NimbleOptions.docs(@opts)
  end

  defstruct [:name]

  def named(name), do: %__MODULE__{name: name}

  def start_link(opts) do
    Options.validate!(opts)
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  defimpl Equinox.Cache do
    @impl Equinox.Cache
    def get(cache, stream_name, max_age) do
      case :ets.lookup(cache.name, stream_name) do
        [{_, _ttl_key, stream_state, insert_time}] ->
          case {max_age, System.monotonic_time(:millisecond) - insert_time} do
            {:infinity, _cache_age} ->
              GenServer.cast(cache.name, {:touch, stream_name})
              stream_state

            {max_age, cache_age} when max_age > cache_age ->
              GenServer.cast(cache.name, {:touch, stream_name})
              stream_state

            _ ->
              nil
          end

        [] ->
          nil
      end
    end

    @impl Equinox.Cache
    def put(cache, stream_name, state) do
      GenServer.call(cache.name, {:insert, stream_name, state})
    end
  end

  use GenServer

  @impl GenServer
  def init(opts) do
    state = %{
      cache_table: opts[:name],
      ttl_table: :"#{opts[:name]}.TTL",
      max_size: opts[:max_size],
      max_memory:
        case opts[:max_memory] do
          :infinity -> :infinity
          bytes when is_integer(bytes) -> bytes
          {kb, :kb} -> kb * 1_000
          {mb, :mb} -> mb * 1_000 * 1_000
          {gb, :gb} -> gb * 1_000 * 1_000 * 1_000
        end
    }

    :ets.new(state.ttl_table, [:named_table, :ordered_set, :private])
    :ets.new(state.cache_table, [:named_table, :set, :public, {:read_concurrency, true}])

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:insert, stream_name, stream_state}, _from, state) do
    insert_cache(state, stream_name, stream_state)
    evict_oversize(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_cast({:touch, stream_name}, state) do
    touch_cache(state, stream_name)
    {:noreply, state}
  end

  defp insert_cache(%{cache_table: cache} = state, stream_name, stream_state) do
    delete_ttl(state, stream_name)
    ttl_key = insert_ttl(state, stream_name)
    :ets.insert(cache, {stream_name, ttl_key, stream_state, System.monotonic_time(:millisecond)})
  end

  defp touch_cache(%{cache_table: cache} = state, stream_name) do
    delete_ttl(state, stream_name)
    new_ttl_key = insert_ttl(state, stream_name)
    :ets.update_element(cache, stream_name, [{2, new_ttl_key}])
  end

  defp insert_ttl(%{ttl_table: ttl_table}, stream_name) do
    ttl_key = :erlang.unique_integer([:monotonic])
    :ets.insert(ttl_table, {ttl_key, stream_name})
    ttl_key
  end

  defp delete_ttl(%{cache_table: cache, ttl_table: ttls}, stream_name) do
    case :ets.lookup(cache, stream_name) do
      [{_, ttl_key, _state, _insert_time}] -> :ets.delete(ttls, ttl_key)
      [] -> nil
    end
  end

  defp evict_oversize(%{cache_table: cache, ttl_table: ttls} = settings) do
    if exhausted_max_size?(settings) or exhausted_max_memory?(settings) do
      case :ets.first(ttls) do
        :"$end_of_table" ->
          nil

        oldest_ttl_key ->
          [{_, oldest_cache_key}] = :ets.lookup(ttls, oldest_ttl_key)
          :ets.delete(ttls, oldest_ttl_key)
          :ets.delete(cache, oldest_cache_key)
          evict_oversize(settings)
      end
    end
  end

  defp exhausted_max_size?(%{max_size: :infinity}), do: false

  defp exhausted_max_size?(%{cache_table: cache, max_size: max_size}) do
    :ets.info(cache, :size) > max_size
  end

  defp exhausted_max_memory?(%{max_memory: :infinity}), do: false

  defp exhausted_max_memory?(%{cache_table: cache, max_memory: max_memory}) do
    :ets.info(cache, :memory) * :erlang.system_info(:wordsize) > max_memory
  end
end
