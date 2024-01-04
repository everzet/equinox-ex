defmodule Equinox.Decider do
  alias Equinox.{Store, State, Codec, Fold, Telemetry}
  alias Equinox.Decider.{Options, Query, Decision, Async, Errors}

  @enforce_keys [:stream_name, :store, :codec, :fold]
  defstruct [
    :stream_name,
    :state,
    :store,
    :codec,
    :fold,
    :context,
    :max_load_attempts,
    :max_sync_attempts,
    :max_resync_attempts
  ]

  @type t :: %__MODULE__{
          stream_name: Store.stream_name(),
          state: not_loaded :: nil | State.t(),
          store: Store.t(),
          codec: Codec.t(),
          fold: Fold.t(),
          context: context(),
          max_load_attempts: pos_integer(),
          max_sync_attempts: pos_integer(),
          max_resync_attempts: non_neg_integer()
        }
  @type context :: map()

  @spec for_stream(String.t(), Options.t()) :: t()
  def for_stream(stream_name, opts) do
    opts = Options.validate!(opts)
    struct(__MODULE__, [{:stream_name, stream_name} | opts])
  end

  @spec loaded?(t()) :: boolean()
  def loaded?(%__MODULE__{state: %State{}}), do: true
  def loaded?(%__MODULE__{state: _}), do: false

  @spec load(t()) :: t()
  def load(%__MODULE__{} = decider), do: load_with_retry(decider)

  @spec load(String.t(), Options.t()) :: t()
  def load(stream_name, opts) when is_bitstring(stream_name) do
    stream_name
    |> for_stream(opts)
    |> load()
  end

  @spec async(t(), Async.Options.t()) :: Async.t()
  def async(%__MODULE__{} = decider, opts), do: Async.wrap_decider(decider, opts)

  @spec async(String.t(), [Options.o() | Async.Options.o()]) :: Async.t()
  def async(stream_name, opts) when is_bitstring(stream_name) do
    {decider_opts, async_opts} = Keyword.split(opts, Options.keys())

    stream_name
    |> for_stream(decider_opts)
    |> async(async_opts)
  end

  @spec start(Async.t()) :: Async.t() | pid()
  def start(%Async{} = async), do: Async.start(async)

  @spec start(t(), Async.Options.t()) :: Async.t() | pid()
  def start(%__MODULE__{} = decider, async_opts) do
    decider
    |> async(async_opts)
    |> start()
  end

  @spec start(String.t(), [Options.o() | Async.Options.o()]) :: Async.t() | pid()
  def start(stream_name, both_opts) when is_bitstring(stream_name) do
    stream_name
    |> async(both_opts)
    |> start()
  end

  @spec query(pid(), Query.t()) :: any()
  def query(pid, query) when is_pid(pid), do: Async.query(pid, query)

  @spec query(Async.t(), Query.t()) :: any()
  def query(%Async{} = async, query), do: Async.query(async, query)

  @spec query(t(), Query.t()) :: any()
  def query(%__MODULE__{} = decider, query_fun) do
    decider = if(not loaded?(decider), do: load(decider), else: decider)

    Telemetry.span_decider_query(decider, query_fun, fn ->
      Query.execute(query_fun, decider.state)
    end)
  end

  def transact(decider_or_async, decision, transact_context \\ %{})

  @spec transact(pid(), Decision.t(), context()) :: {:ok, pid()} | {:error, term()}
  def transact(pid, decision, ctx) when is_pid(pid), do: Async.transact(pid, decision, ctx)

  @spec transact(Async.t(), Decision.t(), context()) :: {:ok, Async.t()} | {:error, term()}
  def transact(%Async{} = async, decision, ctx), do: Async.transact(async, decision, ctx)

  @spec transact(t(), Decision.t(), context()) :: {:ok, t()} | {:error, term()}
  def transact(%__MODULE__{} = decider, decision_fun, transact_context) do
    decider = if(not loaded?(decider), do: load(decider), else: decider)
    context = Map.merge(decider.context, transact_context)

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
      case sync_with_retry(decider, events, context) do
        %__MODULE__{} = synced_decider ->
          {:ok, synced_decider}

        {:conflict, version_conflict} ->
          if resync_attempt >= decider.max_resync_attempts do
            raise Errors.ExhaustedResyncAttempts,
              stream_name: decider.stream_name,
              exception: version_conflict,
              attempts: resync_attempt
          end

          resynced_decider =
            Telemetry.span_decider_resync(decider, resync_attempt, fn ->
              load_without_retry(decider)
            end)

          transact_with_resync(resynced_decider, decision_fun, context, resync_attempt + 1)
      end
    end
  end

  defp sync_with_retry(decider, events, context, sync_attempt \\ 1)

  defp sync_with_retry(decider, [], _context, _sync_attempt), do: decider

  defp sync_with_retry(%__MODULE__{} = decider, events, context, sync_attempt) do
    Telemetry.span_decider_sync(decider, events, sync_attempt, fn ->
      synced_state =
        decider.stream_name
        |> decider.store.sync!(decider.state, events, context, decider.codec, decider.fold)

      %{decider | state: synced_state}
    end)
  rescue
    version_conflict in [Store.Errors.StreamVersionConflict] ->
      {:conflict, version_conflict}

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

      sync_with_retry(decider, events, context, sync_attempt + 1)
  end

  defp load_with_retry(%__MODULE__{} = decider, load_attempt \\ 1) do
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

      load_with_retry(decider, load_attempt + 1)
  end

  defp load_without_retry(%__MODULE__{} = decider) do
    load_with_retry(decider, decider.max_load_attempts)
  end
end
