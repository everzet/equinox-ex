defmodule Equinox.Decider.Stateless do
  alias Equinox.{Decider, State, Store, Codec, Fold, Telemetry}
  alias Equinox.Decider.Actions.{Query, Decision}
  alias Equinox.Decider.Errors

  @enforce_keys [:stream_name, :store, :codec, :fold]
  defstruct state: :not_loaded,
            context: %{},
            stream_name: nil,
            store: nil,
            codec: nil,
            fold: nil,
            max_load_attempts: 2,
            max_sync_attempts: 2,
            max_resync_attempts: 1

  @type t :: %__MODULE__{}

  defmodule Options do
    @opts NimbleOptions.new!(
            store: [
              type: {:custom, __MODULE__, :validate_store, []},
              required: true,
              doc: "Persistence module that implements `Equinox.Store` behaviour"
            ],
            codec: [
              type: {:custom, __MODULE__, :validate_codec, []},
              required: true,
              doc: "Event (en|de)coding module that implements `Equinox.Codec` behaviour"
            ],
            fold: [
              type: {:custom, __MODULE__, :validate_fold, []},
              required: true,
              doc: "State generation module that implements `Equinox.Fold` behaviour"
            ],
            context: [
              type: :map,
              doc: "Decider-wide context. Merged with context passed explicitly via transact"
            ],
            max_load_attempts: [
              type: :pos_integer,
              doc: "How many times (in total) should we try to load the state on load errors"
            ],
            max_sync_attempts: [
              type: :pos_integer,
              doc: "How many times (in total) should we try to sync the state on write errors"
            ],
            max_resync_attempts: [
              type: :non_neg_integer,
              doc: "How many times should we try to resync the state on version conflict"
            ]
          )

    @type t :: [o()]
    @type o :: unquote(NimbleOptions.option_typespec(@opts))

    def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
    def docs, do: NimbleOptions.docs(@opts)

    def validate_store(store) do
      case store do
        store when is_atom(store) ->
          Code.ensure_loaded(store)

          if function_exported?(store, :load!, 4) do
            {:ok, store}
          else
            {:error, "must be a module implementing `Equinox.Store`, but got #{inspect(store)}"}
          end

        _ ->
          {:error, "must be a module implementing `Equinox.Store`, but got #{inspect(store)}"}
      end
    end

    def validate_codec(codec) do
      case codec do
        codec when is_atom(codec) ->
          Code.ensure_loaded(codec)

          if function_exported?(codec, :encode, 2) do
            {:ok, codec}
          else
            {:error, "must be a module implementing `Equinox.Codec`, but got #{inspect(codec)}"}
          end

        _ ->
          {:error, "must be a module implementing `Equinox.Codec`, but got #{inspect(codec)}"}
      end
    end

    def validate_fold(fold) do
      case fold do
        fold when is_atom(fold) ->
          Code.ensure_loaded(fold)

          if function_exported?(fold, :evolve, 2) do
            {:ok, fold}
          else
            {:error, "must be a module implementing `Equinox.Fold`, but got #{inspect(fold)}"}
          end

        _ ->
          {:error, "must be a module implementing `Equinox.Fold`, but got #{inspect(fold)}"}
      end
    end
  end

  @spec for_stream(String.t(), Options.t()) :: t()
  def for_stream(stream_name, opts) do
    opts = Options.validate!(opts)
    struct(__MODULE__, [{:stream_name, stream_name} | opts])
  end

  @spec loaded?(t()) :: boolean()
  def loaded?(%__MODULE__{state: %State{}}), do: true
  def loaded?(%__MODULE__{state: _}), do: false

  @spec load(t()) :: t()
  def load(%__MODULE__{} = decider) do
    load_state_with_retry(decider)
  end

  @spec query(t(), Query.t()) :: any()
  def query(%__MODULE__{} = decider, query_fun) do
    decider = if(not loaded?(decider), do: load(decider), else: decider)

    Telemetry.span_decider_query(decider, query_fun, fn ->
      Query.execute(query_fun, decider.state)
    end)
  end

  @spec transact(t(), Decision.t(), Decider.context()) :: {:ok, t()} | {:error, term()}
  def transact(%__MODULE__{} = decider, decision_fun, context \\ %{}) do
    decider = if(not loaded?(decider), do: load(decider), else: decider)
    context = Map.merge(decider.context, context)

    Telemetry.span_decider_transact(decider, decision_fun, context, fn ->
      transact_with_resync(decider, decision_fun, context)
    end)
  end

  defp transact_with_resync(%__MODULE__{} = decider, decision_fun, context, resync_attempt \\ 0) do
    decision_result =
      Telemetry.span_decider_decision(decider, decision_fun, context, resync_attempt, fn ->
        Decision.execute(decision_fun, decider.state)
      end)

    with {:ok, events} <- decision_result do
      try do
        {:ok, sync_state_with_retry(decider, events, context)}
      rescue
        version_conflict in [Store.Errors.StreamVersionConflict] ->
          if resync_attempt >= decider.max_resync_attempts do
            reraise Errors.ExhaustedResyncAttempts,
                    [
                      stream_name: decider.stream_name,
                      exception: version_conflict,
                      attempts: resync_attempt
                    ],
                    __STACKTRACE__
          end

          resynced_decider =
            Telemetry.span_decider_resync(decider, resync_attempt, fn ->
              load_state_without_retry(decider)
            end)

          transact_with_resync(resynced_decider, decision_fun, context, resync_attempt + 1)
      end
    end
  end

  defp sync_state_with_retry(decider, events, context, sync_attempt \\ 1)

  defp sync_state_with_retry(decider, [], _context, _sync_attempt), do: decider

  defp sync_state_with_retry(%__MODULE__{} = decider, events, context, sync_attempt) do
    try do
      Telemetry.span_decider_sync(decider, events, sync_attempt, fn ->
        synced_state =
          decider.stream_name
          |> decider.store.sync!(decider.state, events, context, decider.codec, decider.fold)

        %{decider | state: synced_state}
      end)
    rescue
      version_conflict in [Store.Errors.StreamVersionConflict] ->
        reraise version_conflict, __STACKTRACE__

      unrecoverable in [Codec.Errors.EncodeError, Fold.Errors.EvolveError] ->
        reraise unrecoverable, __STACKTRACE__

      recoverable ->
        if sync_attempt >= decider.max_sync_attempts do
          reraise Errors.ExhaustedSyncAttempts,
                  [
                    stream_name: decider.stream_name,
                    exception: recoverable,
                    attempts: sync_attempt
                  ],
                  __STACKTRACE__
        end

        sync_state_with_retry(decider, events, context, sync_attempt + 1)
    end
  end

  defp load_state_with_retry(%__MODULE__{} = decider, load_attempt \\ 1) do
    try do
      Telemetry.span_decider_load(decider, load_attempt, fn ->
        current_state =
          if(not loaded?(decider), do: State.init(decider.fold), else: decider.state)

        loaded_state =
          decider.stream_name
          |> decider.store.load!(current_state, decider.codec, decider.fold)

        %{decider | state: loaded_state}
      end)
    rescue
      unrecoverable in [Codec.Errors.DecodeError, Fold.Errors.EvolveError] ->
        reraise unrecoverable, __STACKTRACE__

      recoverable ->
        if load_attempt >= decider.max_load_attempts do
          reraise Errors.ExhaustedLoadAttempts,
                  [
                    stream_name: decider.stream_name,
                    exception: recoverable,
                    attempts: load_attempt
                  ],
                  __STACKTRACE__
        end

        load_state_with_retry(decider, load_attempt + 1)
    end
  end

  defp load_state_without_retry(%__MODULE__{} = decider) do
    load_state_with_retry(decider, decider.max_load_attempts)
  end
end
