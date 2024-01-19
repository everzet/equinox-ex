defmodule Equinox.MessageDb.Store.LatestKnownEvent do
  defmodule Options do
    @opts NimbleOptions.new!(
            conn: [
              type:
                {:or,
                 [
                   :atom,
                   {:struct, DBConnection},
                   keyword_list: [
                     leader: [type: {:or, [:atom, {:struct, DBConnection}]}, required: true],
                     follower: [type: {:or, [:atom, {:struct, DBConnection}]}, required: true]
                   ]
                 ]},
              required: true,
              doc: "Database connection(s) to a leader and follower DBs"
            ],
            cache: [
              type: :atom,
              default: Equinox.Cache.NoCache,
              doc: "State caching module that implements `Equinox.Cache` behaviour"
            ],
            codec: [
              type: :atom,
              required: true,
              doc: "Event (en|de)coding module that implements `Equinox.Codec` behaviour"
            ],
            fold: [
              type: :atom,
              required: true,
              doc: "State producing module that implements `Equinox.Fold` behaviour"
            ]
          )

    @type t :: [o()]
    @type o :: unquote(NimbleOptions.option_typespec(@opts))

    @spec docs() :: String.t()
    def docs, do: NimbleOptions.docs(@opts)

    @spec validate!(t) :: t()
    def validate!(opts), do: NimbleOptions.validate!(opts, @opts)

    @spec merge(t(), t()) :: t()
    def merge(o1, o2), do: Keyword.merge(o1, o2)

    @spec conn(t(), :leader | :follower) :: module()
    def conn(opts, type) do
      case opts[:conn] do
        list when is_list(list) -> list[type]
        conn when is_atom(conn) -> conn
        conn when is_struct(conn, DBConnection) -> conn
      end
    end
  end

  defmacro __using__(opts) do
    quote do
      @behaviour Equinox.Store
      @opts unquote(opts)

      alias Equinox.MessageDb.Store.LatestKnownEvent

      def sync(stream, state, to_sync, opts),
        do: LatestKnownEvent.sync(stream, state, to_sync, Options.merge(@opts, opts))

      def load(stream, policy, opts),
        do: LatestKnownEvent.load(stream, policy, Options.merge(@opts, opts))

      defoverridable Equinox.Store
    end
  end

  @behaviour Equinox.Store

  @impl Equinox.Store
  def load(stream, policy, opts) do
    opts = Options.validate!(opts)
    init = Equinox.Store.State.init(opts[:fold], -1)

    if policy.assumes_empty? do
      {:ok, init}
    else
      case opts[:cache].fetch(stream, policy.max_cache_age) do
        nil ->
          opts
          |> Options.conn(if(policy.requires_leader?, do: :leader, else: :follower))
          |> do_load(stream, init, opts[:cache], opts[:codec], opts[:fold])

        val ->
          {:ok, val}
      end
    end
  end

  @impl Equinox.Store
  def sync(stream, state, to_sync, opts) do
    opts = Options.validate!(opts)

    opts
    |> Options.conn(:leader)
    |> do_sync(stream, state, to_sync, opts[:cache], opts[:codec], opts[:fold])
  end

  defp do_load(conn, stream, state, cache, codec, fold) do
    case Equinox.MessageDb.Store.load_latest_known_event(conn, stream, state, codec, fold) do
      {:ok, state} -> {:ok, tap(state, &cache.insert(stream, &1))}
      anything_else -> anything_else
    end
  end

  defp do_sync(conn, stream, state, to_sync, cache, codec, fold) do
    case Equinox.MessageDb.Store.sync(conn, stream, state, to_sync, codec, fold) do
      {:error, %Equinox.MessageDb.Writer.StreamVersionConflict{}} ->
        {:conflict, fn -> do_load(conn, stream, state, cache, codec, fold) end}

      {:ok, new_state} ->
        {:ok, tap(new_state, &cache.insert(stream, &1))}

      anything_else ->
        anything_else
    end
  end
end
