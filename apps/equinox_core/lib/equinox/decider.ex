defmodule Equinox.Decider do
  alias Equinox.{Store, Telemetry}

  alias Equinox.Decider.{
    Options,
    Decision,
    Query,
    Async,
    ExhaustedLoadAttempts,
    ExhaustedSyncAttempts,
    ExhaustedResyncAttempts
  }

  @enforce_keys [:stream_name, :store]
  defstruct [
    :stream_name,
    :store,
    :state,
    :max_load_attempts,
    :max_sync_attempts,
    :max_resync_attempts
  ]

  @type t :: %__MODULE__{
          stream_name: Store.stream_name(),
          store: Store.t() | {Store.t(), Store.options()},
          state: nil | Store.State.t(),
          max_load_attempts: pos_integer(),
          max_sync_attempts: pos_integer(),
          max_resync_attempts: non_neg_integer()
        }

  @spec for_stream(String.t(), Options.t()) :: t()
  def for_stream(stream_name, opts) do
    opts = Options.validate!(opts)
    struct(__MODULE__, [{:stream_name, stream_name} | opts])
  end

  @spec loaded?(t()) :: boolean()
  def loaded?(%__MODULE__{state: state}), do: Store.State.initialized?(state)

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

  @spec query(pid(), Query.t()) :: {any(), pid()}
  def query(pid, query) when is_pid(pid), do: Async.query(pid, query)

  @spec query(Async.t(), Query.t()) :: {any(), Async.t()}
  def query(%Async{} = async, query), do: Async.query(async, query)

  @spec query(t(), Query.t()) :: {any(), t()}
  def query(%__MODULE__{} = decider, query), do: make_query(decider, query)

  defp make_query(%__MODULE__{} = decider, query) do
    Telemetry.span_decider_query(decider, query, fn ->
      decider = if(not loaded?(decider), do: load(decider), else: decider)
      {Query.execute(query, decider.state.value), decider}
    end)
  end

  def transact(decider_or_async, decision, context \\ %{})

  @spec transact(pid(), Decision.t(), Store.sync_context()) ::
          {:ok, pid()} | {:error, term(), pid()}
  def transact(pid, decision, ctx) when is_pid(pid), do: Async.transact(pid, decision, ctx)

  @spec transact(Async.t(), Decision.t(), Store.sync_context()) ::
          {:ok, Async.t()} | {:error, term(), Async.t()}
  def transact(%Async{} = async, decision, ctx), do: Async.transact(async, decision, ctx)

  @spec transact(t(), Decision.t(), Store.sync_context()) :: {:ok, t()} | {:error, term(), t()}
  def transact(%__MODULE__{} = decider, decision, ctx), do: do_transact(decider, decision, ctx)

  defp do_transact(%__MODULE__{} = decider, decision, context) do
    Telemetry.span_decider_transact(decider, decision, context, fn ->
      decider = if(not loaded?(decider), do: load(decider), else: decider)

      case transact_with_resync(decider, decision, context) do
        {:ok, decider} -> {:ok, decider}
        {:error, error} -> {:error, error, decider}
      end
    end)
  end

  defp transact_with_resync(%__MODULE__{} = decider, decision, context, attempt \\ 0) do
    with {:ok, decision_outcome} <- make_decision(decider, decision, context, attempt),
         {:ok, synced_decider} <- sync_with_retry(decider, decision_outcome) do
      {:ok, synced_decider}
    else
      {:error, %Decision.Error{} = decision_error} ->
        {:error, decision_error}

      {:error, %Store.StreamVersionConflict{} = version_conflict} ->
        if attempt >= decider.max_resync_attempts do
          raise ExhaustedResyncAttempts,
            stream_name: decider.stream_name,
            exception: version_conflict,
            attempts: attempt
        end

        decider
        |> do_resync(attempt)
        |> transact_with_resync(decision, context, attempt + 1)
    end
  end

  defp make_decision(%__MODULE__{} = decider, decision, context, attempt) do
    Telemetry.span_transact_decision(decider, decision, context, attempt, fn ->
      with {:ok, events} <- Decision.execute(decision, decider.state.value) do
        {:ok, Store.Outcome.new(events, context)}
      end
    end)
  end

  defp do_resync(%__MODULE__{} = decider, attempt) do
    Telemetry.span_transact_resync(decider, attempt, fn ->
      load_with_retry(decider)
    end)
  end

  defp load_state(%__MODULE__{state: state} = decider, attempt) do
    Telemetry.span_load_state(decider, attempt, fn ->
      case decider.store do
        {store, opts} -> store.load(decider.stream_name, state, opts)
        store when is_atom(store) -> store.load(decider.stream_name, state, [])
      end
    end)
  end

  defp load_with_retry(%__MODULE__{} = decider, attempt \\ 1) do
    with {:ok, loaded_state} <- load_state(decider, attempt) do
      set_state(decider, loaded_state)
    else
      {:error, maybe_recoverable_exception, partially_loaded_state} ->
        if attempt >= decider.max_load_attempts do
          raise ExhaustedLoadAttempts,
            stream_name: decider.stream_name,
            exception: maybe_recoverable_exception,
            attempts: attempt
        end

        decider
        |> set_state(partially_loaded_state)
        |> load_with_retry(attempt + 1)
    end
  end

  defp sync_with_retry(%__MODULE__{} = decider, outcome, attempt \\ 1) do
    with {:ok, synced_state} <- sync_state(decider, outcome, attempt) do
      {:ok, set_state(decider, synced_state)}
    else
      {:error, %Store.StreamVersionConflict{} = version_conflict} ->
        {:error, version_conflict}

      {:error, maybe_recoverable_exception} ->
        if attempt >= decider.max_sync_attempts do
          raise ExhaustedSyncAttempts,
            stream_name: decider.stream_name,
            exception: maybe_recoverable_exception,
            attempts: attempt
        end

        sync_with_retry(decider, outcome, attempt + 1)
    end
  end

  defp sync_state(%__MODULE__{state: state} = decider, outcome, attempt) do
    Telemetry.span_sync_state(decider, outcome, attempt, fn ->
      case {outcome.events, decider.store} do
        {[], _} -> {:ok, state}
        {_, {store, opts}} -> store.sync(decider.stream_name, state, outcome, opts)
        {_, store} when is_atom(store) -> store.sync(decider.stream_name, state, outcome, [])
      end
    end)
  end

  defp set_state(%__MODULE__{} = decider, new_state), do: %{decider | state: new_state}
end
