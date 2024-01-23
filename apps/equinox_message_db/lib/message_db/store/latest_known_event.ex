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
              type: {:or, [:any, {:tuple, [:atom, :keyword_list]}]},
              default: Equinox.Cache.NoCache.new(),
              doc:
                "Implementation of `Equinox.Cache` protocol or module and options producing one"
            ],
            codec: [
              type: {:or, [:any, {:tuple, [:atom, :keyword_list]}]},
              required: true,
              doc:
                "Implementation of `Equinox.Codec` behaviour or module and options producing one"
            ],
            fold: [
              type: {:or, [:any, {:tuple, [:atom, :keyword_list]}]},
              required: true,
              doc:
                "Implementation of `Equinox.Fold` behaviour or module and options producing one"
            ]
          )

    @type t :: [o()]
    @type o :: unquote(NimbleOptions.option_typespec(@opts))

    def docs, do: NimbleOptions.docs(@opts)

    def validate!(opts) do
      opts
      |> NimbleOptions.validate!(@opts)
      |> Keyword.update!(:cache, &apply_new/1)
      |> Keyword.update!(:codec, &apply_new/1)
      |> Keyword.update!(:fold, &apply_new/1)
      |> set_conns()
    end

    defp apply_new({m, o}), do: apply(m, :new, [o])
    defp apply_new(not_new), do: not_new

    defp set_conns(opts) do
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

  def new(opts), do: struct(__MODULE__, Options.validate!(opts))

  defimpl Equinox.Store do
    alias Equinox.MessageDb.Store.LatestKnownEvent

    @impl Equinox.Store
    def load(%LatestKnownEvent{} = store, stream, policy) do
      init = Equinox.Store.State.init(store.fold, -1)

      if policy.assumes_empty? do
        {:ok, init}
      else
        case Equinox.Cache.get(store.cache, stream, policy.max_cache_age) do
          nil ->
            if(policy.requires_leader?, do: store.leader, else: store.follower)
            |> do_load(stream, init, store.cache, store.codec, store.fold)

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
        {:ok, state} -> {:ok, tap(state, &Equinox.Cache.put(cache, stream, &1))}
        anything_else -> anything_else
      end
    end

    defp do_sync(conn, stream, state, to_sync, cache, codec, fold) do
      case Equinox.MessageDb.Store.sync(conn, stream, state, to_sync, codec, fold) do
        {:error, %Equinox.MessageDb.Writer.StreamVersionConflict{}} ->
          {:conflict, fn -> do_load(conn, stream, state, cache, codec, fold) end}

        {:ok, new_state} ->
          {:ok, tap(new_state, &Equinox.Cache.put(cache, stream, &1))}

        anything_else ->
          anything_else
      end
    end
  end
end
