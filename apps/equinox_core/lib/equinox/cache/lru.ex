defmodule Equinox.Cache.LRU do
  defstruct [:name]

  def new(name), do: %__MODULE__{name: name}

  defimpl Equinox.Cache do
    @impl Equinox.Cache
    def fetch(cache, stream_name, max_age) do
      case :ets.lookup(cache.name, stream_name) do
        [{_, _ttl_key, stream_state, insert_time}] ->
          GenServer.cast(cache.name, {:touch, stream_name})
          unless(age(insert_time) > max_age, do: stream_state)

        [] ->
          nil
      end
    end

    @impl Equinox.Cache
    def insert(cache, stream_name, state) do
      GenServer.call(cache.name, {:insert, stream_name, state})
    end

    defp age(insert_time), do: System.monotonic_time() - insert_time
  end

  defmodule Options do
    @opts NimbleOptions.new!(
            name: [
              type: :atom,
              required: true,
              doc: "Name of the cache process and ETS table"
            ],
            max_size: [
              type: :pos_integer,
              required: true,
              doc:
                "Maximum number of stored items. Every time this number reached - the least touched item is evicted"
            ],
            max_memory: [
              type: :pos_integer,
              required: true,
              doc:
                "Maximum total consumed memory in bytes. Every time this number reached - the least touched item is evicted"
            ]
          )

    @type t :: [o()]
    @type o :: unquote(NimbleOptions.option_typespec(@opts))

    def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
    def docs, do: NimbleOptions.docs(@opts)
    def keys, do: Keyword.keys(@opts.schema)
  end

  use GenServer

  def start_link(opts) do
    Options.validate!(opts)
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @impl GenServer
  def init(opts) do
    state = %{
      cache_table: opts[:name],
      ttl_table: :"#{opts[:name]}.TTL",
      max_size: opts[:max_size],
      max_memory: opts[:max_memory]
    }

    :ets.new(state.ttl_table, [:named_table, :ordered_set, :private])
    :ets.new(state.cache_table, [:named_table, :set, :public])

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:insert, stream_name, stream_state}, _from, state) do
    delete_ttl(state, stream_name)
    ttl_key = insert_ttl(state, stream_name)
    insert_cache(state, stream_name, ttl_key, stream_state)
    evict_oversize(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_cast({:touch, stream_name}, state) do
    delete_ttl(state, stream_name)
    ttl_key = insert_ttl(state, stream_name)
    update_ttl_key(state, stream_name, ttl_key)
    {:noreply, state}
  end

  defp insert_cache(%{cache_table: cache}, stream_name, ttl_key, stream_state) do
    :ets.insert(cache, {stream_name, ttl_key, stream_state, System.monotonic_time()})
  end

  defp update_ttl_key(%{cache_table: cache}, stream_name, ttl_key) do
    :ets.update_element(cache, stream_name, [{2, ttl_key}])
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
    size = :ets.info(cache, :size)
    memory = :ets.info(cache, :memory) * :erlang.system_info(:wordsize)

    if size > settings.max_size or memory > settings.max_memory do
      oldest_ttl_key = :ets.first(ttls)
      [{_, oldest_cache_key}] = :ets.lookup(ttls, oldest_ttl_key)
      :ets.delete(ttls, oldest_ttl_key)
      :ets.delete(cache, oldest_cache_key)
    end
  end
end
