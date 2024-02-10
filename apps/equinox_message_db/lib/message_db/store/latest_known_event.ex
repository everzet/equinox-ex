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
              doc: "Database connection(s) to leader and follower DBs"
            ],
            cache: [
              type: {:or, [{:tuple, [:atom, :keyword_list]}, :mfa]},
              default: {Equinox.Cache.NoCache, []},
              doc: "Builder function returning implementation of `Equinox.Cache` protocol"
            ],
            codec: [
              type: {:or, [:atom, {:tuple, [:atom, :keyword_list]}, :mfa]},
              required: true,
              doc: "Implementation of `Equinox.Codec` behaviour or builder function returning one"
            ],
            fold: [
              type: {:or, [:atom, {:tuple, [:atom, :keyword_list]}, :mfa]},
              required: true,
              doc: "Implementation of `Equinox.Fold` behaviour or builder function returning one"
            ]
          )

    @type t :: [o()]
    @type o :: unquote(NimbleOptions.option_typespec(@opts))

    def docs, do: NimbleOptions.docs(@opts)

    def validate!(opts) do
      opts
      |> NimbleOptions.validate!(@opts)
      |> Keyword.update!(:cache, &init_dep/1)
      |> Keyword.update!(:codec, &init_dep/1)
      |> Keyword.update!(:fold, &init_dep/1)
      |> init_conns()
    end

    defp init_dep(a) when is_atom(a), do: a
    defp init_dep({m, o}), do: m.new(o)
    defp init_dep({m, f, a}), do: apply(m, f, a)

    defp init_conns(opts) do
      {conn, opts} = Keyword.pop!(opts, :conn)

      {leader, follower} =
        case conn do
          conn when is_list(conn) -> {conn[:leader], conn[:follower]}
          conn -> {conn, conn}
        end

      [{:leader, leader}, {:follower, follower} | opts]
    end
  end

  @enforce_keys [:leader, :follower, :cache, :codec, :fold]
  defstruct [:leader, :follower, :cache, :codec, :fold]

  @type t :: %__MODULE__{
          leader: Postgrex.conn(),
          follower: Postgrex.conn(),
          cache: Equinox.Cache.t(),
          codec: Equinox.Codec.t(),
          fold: Equinox.Fold.t()
        }

  def new(opts), do: struct(__MODULE__, Options.validate!(opts))

  defimpl Equinox.Store do
    alias Equinox.{Cache, Store.State}
    alias Equinox.MessageDb.Store.{Base, LatestKnownEvent}
    alias Equinox.MessageDb.Writer.StreamVersionConflictError

    @impl Equinox.Store
    def load(%LatestKnownEvent{} = store, stream, policy) do
      %{leader: l_conn, follower: f_conn, cache: cache, codec: codec, fold: fold} = store
      empty = State.init(store.fold, -1)

      cond do
        policy.assumes_empty? -> {:ok, empty}
        cached = Cache.get(store.cache, stream, policy.max_cache_age) -> {:ok, cached}
        policy.requires_leader? -> do_load(l_conn, stream, empty, cache, codec, fold)
        :otherwise -> do_load(f_conn, stream, empty, cache, codec, fold)
      end
    end

    @impl Equinox.Store
    def sync(%LatestKnownEvent{} = store, stream, state, events) do
      %{leader: conn, cache: cache, codec: codec, fold: fold} = store
      do_sync(conn, stream, state, events, cache, codec, fold)
    end

    defp do_load(conn, stream, state, cache, codec, fold) do
      case Base.load_latest_known_event(conn, stream, state, codec, fold) do
        {:ok, state} -> {:ok, tap(state, &Cache.put(cache, stream, &1))}
        anything_else -> anything_else
      end
    end

    defp do_sync(conn, stream, state, events, cache, codec, fold) do
      resync_fun = fn -> do_load(conn, stream, state, cache, codec, fold) end

      case Base.sync(conn, stream, state, events, codec, fold) do
        {:error, %StreamVersionConflictError{}} -> {:conflict, resync_fun}
        {:ok, new_state} -> {:ok, tap(new_state, &Cache.put(cache, stream, &1))}
        anything_else -> anything_else
      end
    end
  end
end
