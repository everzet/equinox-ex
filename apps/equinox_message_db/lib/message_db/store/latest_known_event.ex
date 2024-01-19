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
              type: :any,
              default: Equinox.Cache.NoCache.new(),
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

    def docs, do: NimbleOptions.docs(@opts)
    def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
  end

  @enforce_keys [:leader, :follower, :cache, :codec, :fold]
  defstruct [:leader, :follower, :cache, :codec, :fold]

  def new(opts) do
    {conn, opts} = opts |> Options.validate!() |> Keyword.pop(:conn)

    {leader, follower} =
      case conn do
        conn when is_list(conn) -> {conn[:leader], conn[:follower]}
        conn -> {conn, conn}
      end

    struct(__MODULE__, [{:leader, leader}, {:follower, follower} | opts])
  end

  defimpl Equinox.Store do
    alias Equinox.MessageDb.Store.LatestKnownEvent

    @impl Equinox.Store
    def load(%LatestKnownEvent{} = store, stream, policy) do
      init = Equinox.Store.State.init(store.fold, -1)

      if policy.assumes_empty? do
        {:ok, init}
      else
        case Equinox.Cache.fetch(store.cache, stream, policy.max_cache_age) do
          nil ->
            conn = if(policy.requires_leader?, do: store.leader, else: store.follower)
            do_load(conn, stream, init, store.cache, store.codec, store.fold)

          val ->
            {:ok, val}
        end
      end
    end

    @impl Equinox.Store
    def sync(%LatestKnownEvent{} = store, stream, state, to_sync) do
      do_sync(store.leader, stream, state, to_sync, store.cache, store.codec, store.fold)
    end

    defp do_load(conn, stream, state, cache, codec, fold) do
      case Equinox.MessageDb.Store.load_latest_known_event(conn, stream, state, codec, fold) do
        {:ok, state} -> {:ok, tap(state, &Equinox.Cache.insert(cache, stream, &1))}
        anything_else -> anything_else
      end
    end

    defp do_sync(conn, stream, state, to_sync, cache, codec, fold) do
      case Equinox.MessageDb.Store.sync(conn, stream, state, to_sync, codec, fold) do
        {:error, %Equinox.MessageDb.Writer.StreamVersionConflict{}} ->
          {:conflict, fn -> do_load(conn, stream, state, cache, codec, fold) end}

        {:ok, new_state} ->
          {:ok, tap(new_state, &Equinox.Cache.insert(cache, stream, &1))}

        anything_else ->
          anything_else
      end
    end
  end
end
