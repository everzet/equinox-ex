defmodule Equinox.Decider do
  alias Equinox.{Store, Store.EventsToSync}
  alias Equinox.Decider.{Options, Decision, Query, LoadPolicy, ResyncPolicy, Async}

  @enforce_keys [:stream, :store, :resync, :context]
  defstruct [:stream, :store, :resync, :context]

  @type t :: %__MODULE__{
          stream: Store.stream_name(),
          store: Store.t(),
          resync: ResyncPolicy.t(),
          context: EventsToSync.context()
        }

  @spec for_stream(String.t(), Options.t()) :: t()
  def for_stream(stream_name, opts) do
    struct(__MODULE__, [{:stream, stream_name} | Options.validate!(opts)])
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

  @spec update_context(t(), (EventsToSync.context() -> EventsToSync.context())) :: t()
  @spec update_context(Async.t(), (EventsToSync.context() -> EventsToSync.context())) :: Async.t()
  def update_context(decider, update_fun), do: update_in(decider.context, update_fun)

  @spec query(Async.t(), Query.t(), LoadPolicy.t()) :: term()
  @spec query(t(), Query.t(), LoadPolicy.t()) :: term()
  def query(decider, query, load \\ LoadPolicy.default())

  def query(%Async{} = async, query, load), do: Async.query(async, query, load)

  def query(%__MODULE__{} = decider, query, load) do
    with {:ok, loaded_state} <- load_state(decider, load) do
      Query.execute(query, loaded_state.value)
    else
      {:error, unrecoverable_error} -> raise unrecoverable_error
    end
  end

  @spec transact(Async.t(), Decision.without_result(), LoadPolicy.t()) ::
          :ok | {:error, term()}
  @spec transact(Async.t(), Decision.with_result(), LoadPolicy.t()) ::
          {:ok, term()} | {:error, term()}
  @spec transact(t(), Decision.without_result()) ::
          :ok | {:error, term()}
  @spec transact(t(), Decision.with_result()) ::
          {:ok, term()} | {:error, term()}
  def transact(decider, decision, load \\ LoadPolicy.default())

  def transact(%Async{} = async, decision, load), do: Async.transact(async, decision, load)

  def transact(%__MODULE__{} = decider, decision, load) do
    with {:ok, state} <- load_state(decider, load),
         {:ok, result} <- transact_with_resync(decider, state, decision) do
      result
    else
      {:error, %Decision.Error{} = decision_error} -> {:error, decision_error}
      {:error, unrecoverable_error} -> raise unrecoverable_error
    end
  end

  defp load_state(%__MODULE__{} = decider, policy) do
    Store.load(decider.store, decider.stream, policy)
  end

  defp transact_with_resync(%__MODULE__{} = decider, state, decision, attempt \\ 0) do
    with {:ok, result, to_sync} <- make_decision(state, decision, decider.context),
         {:ok, _synced_state} <- sync_state(decider, state, to_sync) do
      {:ok, result}
    else
      {:error, error} ->
        {:error, error}

      {:conflict, resync_fun} ->
        with :ok <- ResyncPolicy.validate_attempt(decider.resync, attempt),
             {:ok, resynced_state} <- resync_fun.() do
          transact_with_resync(decider, resynced_state, decision, attempt + 1)
        end
    end
  end

  defp make_decision(state, decision, context) do
    case Decision.execute(decision, state.value) do
      {:ok, events} -> {:ok, :ok, EventsToSync.new(events, context)}
      {:ok, result, events} -> {:ok, {:ok, result}, EventsToSync.new(events, context)}
      {:error, error} -> {:error, error}
    end
  end

  defp sync_state(%__MODULE__{} = decider, state, to_sync) do
    if not EventsToSync.empty?(to_sync) do
      Store.sync(decider.store, decider.stream, state, to_sync)
    else
      {:ok, state}
    end
  end
end
