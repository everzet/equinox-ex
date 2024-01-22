defmodule Equinox.MessageDb.Store.Unoptimized do
  alias Equinox.MessageDb.Store.Unoptimized

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
              type: {:or, [:any, :mfa]},
              default: Equinox.Cache.NoCache.new(),
              doc: "Implementation of `Equinox.Cache` protocol or function returning one"
            ],
            codec: [
              type: {:or, [:atom, :mfa]},
              required: true,
              doc: "Module implementing `Equinox.Codec` behaviour or function returning one"
            ],
            fold: [
              type: {:or, [:atom, :mfa]},
              required: true,
              doc: "Module implementing `Equinox.Fold` behaviour or function returning one"
            ],
            batch_size: [
              type: :pos_integer,
              default: 500,
              doc: "Number of events to read per batch from DB"
            ]
          )

    @type t :: [o()]
    @type o :: unquote(NimbleOptions.option_typespec(@opts))

    def docs, do: NimbleOptions.docs(@opts)

    def validate!(opts) do
      opts
      |> NimbleOptions.validate!(@opts)
      |> Keyword.update!(:cache, &apply_mfa/1)
      |> Keyword.update!(:codec, &apply_mfa/1)
      |> Keyword.update!(:fold, &apply_mfa/1)
      |> normalize_conn()
    end

    defp apply_mfa({m, f, a}), do: apply(m, f, a)
    defp apply_mfa(not_mfa), do: not_mfa

    defp normalize_conn(opts) do
      {conn, opts} = Keyword.pop!(opts, :conn)

      {leader, follower} =
        case conn do
          conn when is_list(conn) -> {conn[:leader], conn[:follower]}
          conn -> {conn, conn}
        end

      [{:leader, leader}, {:follower, follower} | opts]
    end
  end

  @enforce_keys [:leader, :follower, :cache, :codec, :fold, :batch_size]
  defstruct [:leader, :follower, :cache, :codec, :fold, :batch_size]

  def new(opts), do: struct(__MODULE__, Options.validate!(opts))

  defimpl Equinox.Store do
    alias Equinox.MessageDb.Store.Unoptimized

    @impl Equinox.Store
    def load(%Unoptimized{} = store, stream, policy) do
      init = Equinox.Store.State.init(store.fold, -1)

      if policy.assumes_empty? do
        {:ok, init}
      else
        case Equinox.Cache.get(store.cache, stream, policy.max_cache_age) do
          nil ->
            if(policy.requires_leader?, do: store.leader, else: store.follower)
            |> do_load(stream, init, store.cache, store.codec, store.fold, store.batch_size)

          val ->
            {:ok, val}
        end
      end
    end

    @impl Equinox.Store
    def sync(%Unoptimized{} = store, stream, state, to_sync) do
      do_sync(
        store.leader,
        stream,
        state,
        to_sync,
        store.cache,
        store.codec,
        store.fold,
        store.batch_size
      )
    end

    defp do_load(conn, stream, state, cache, codec, fold, batch_size) do
      case Equinox.MessageDb.Store.load_unoptimized(conn, stream, state, codec, fold, batch_size) do
        {:ok, state} -> {:ok, tap(state, &Equinox.Cache.put(cache, stream, &1))}
        anything_else -> anything_else
      end
    end

    defp do_sync(conn, stream, state, to_sync, cache, codec, fold, batch_size) do
      case Equinox.MessageDb.Store.sync(conn, stream, state, to_sync, codec, fold) do
        {:error, %Equinox.MessageDb.Writer.StreamVersionConflict{}} ->
          {:conflict, fn -> do_load(conn, stream, state, cache, codec, fold, batch_size) end}

        {:ok, new_state} ->
          {:ok, tap(new_state, &Equinox.Cache.put(cache, stream, &1))}

        anything_else ->
          anything_else
      end
    end
  end
end
