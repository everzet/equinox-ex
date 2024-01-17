defmodule Equinox.Decider do
  alias Equinox.{Store, Telemetry}
  alias Equinox.Decider.{Options, Decision, Query, ResyncPolicy, Async}

  @enforce_keys [:stream_name, :store]
  defstruct [
    :stream_name,
    :store,
    :context,
    :state,
    :resync_policy
  ]

  @type t :: %__MODULE__{
          stream_name: Store.stream_name(),
          store: Store.t() | {Store.t(), Store.options()},
          state: nil | Store.State.t(),
          context: Store.sync_context(),
          resync_policy: ResyncPolicy.t()
        }

  @spec for_stream(String.t(), Options.t()) :: t()
  def for_stream(stream_name, opts) do
    opts = Options.validate!(opts)
    struct(__MODULE__, [{:stream_name, stream_name} | opts])
  end

  @spec loaded?(t()) :: boolean()
  def loaded?(%__MODULE__{state: state}), do: Store.State.initialized?(state)

  @spec load(t()) :: t()
  def load(%__MODULE__{} = decider), do: do_load(decider)

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

  @spec query(Async.t(), Query.t()) :: {term(), Async.t()}
  def query(%Async{} = async, query), do: Async.query(async, query)

  @spec query(t(), Query.t()) :: {term(), t()}
  def query(%__MODULE__{} = decider, query), do: make_query(decider, query)

  defp make_query(%__MODULE__{} = decider, query) do
    Telemetry.span_decider_query(decider, query, fn ->
      decider = if(not loaded?(decider), do: load(decider), else: decider)
      {Query.execute(query, decider.state.value), decider}
    end)
  end

  @spec transact(Async.t(), Decision.without_result()) ::
          {:ok, Async.t()} | {:error, term(), Async.t()}
  @spec transact(Async.t(), Decision.with_result()) ::
          {:ok, term(), Async.t()} | {:error, term(), Async.t()}
  def transact(%Async{} = async, decision), do: Async.transact(async, decision)

  @spec transact(t(), Decision.without_result()) ::
          {:ok, t()} | {:error, term(), t()}
  @spec transact(t(), Decision.with_result()) ::
          {:ok, term(), t()} | {:error, term(), t()}
  def transact(%__MODULE__{} = decider, decision), do: do_transact(decider, decision)

  defp do_transact(%__MODULE__{} = decider, decision) do
    Telemetry.span_decider_transact(decider, decision, fn ->
      decider = if(not loaded?(decider), do: load(decider), else: decider)
      transact_with_resync(decider, decision)
    end)
  end

  defp transact_with_resync(%__MODULE__{} = decider, decision, attempt \\ 0) do
    with {:ok, result, outcome} <- make_decision(decider, decision, attempt),
         {:ok, synced_decider} <- do_sync(decider, outcome) do
      case result do
        {:result, result} -> {:ok, result, synced_decider}
        nil -> {:ok, synced_decider}
      end
    else
      {:error, %Decision.Error{} = decision_error} ->
        {:error, decision_error, decider}

      {:error, %Store.StreamVersionConflict{} = version_conflict} ->
        ResyncPolicy.validate!(decider.resync_policy, attempt, version_conflict)

        decider
        |> do_resync(attempt)
        |> transact_with_resync(decision, attempt + 1)
    end
  end

  defp make_decision(%__MODULE__{} = decider, decision, attempt) do
    Telemetry.span_transact_decision(decider, decision, attempt, fn ->
      case Decision.execute(decision, decider.state.value) do
        {:ok, events} ->
          {:ok, nil, Store.Outcome.new(events, decider.context)}

        {:ok, result, events} ->
          {:ok, {:result, result}, Store.Outcome.new(events, decider.context)}

        {:error, error} ->
          {:error, error}
      end
    end)
  end

  defp do_resync(%__MODULE__{} = decider, attempt) do
    Telemetry.span_transact_resync(decider, attempt, fn ->
      do_load(decider)
    end)
  end

  defp do_load(%__MODULE__{} = decider) do
    policy = Store.LoadPolicy.default()

    with {:ok, loaded_state} <- load_state(decider, policy) do
      set_state(decider, loaded_state)
    else
      {:error, error, _partial_fold} -> raise error
    end
  end

  defp load_state(%__MODULE__{state: state} = decider, policy) do
    Telemetry.span_load_state(decider, policy, fn ->
      case decider.store do
        {store, opts} -> store.load(decider.stream_name, state, policy, opts)
        store when is_atom(store) -> store.load(decider.stream_name, state, [])
      end
    end)
  end

  defp do_sync(%__MODULE__{} = decider, outcome) do
    with {:ok, synced_state} <- sync_state(decider, outcome) do
      {:ok, set_state(decider, synced_state)}
    else
      {:error, %Store.StreamVersionConflict{} = error} -> {:error, error}
      {:error, error} -> raise error
    end
  end

  defp sync_state(%__MODULE__{state: state} = decider, outcome) do
    Telemetry.span_sync_state(decider, outcome, fn ->
      case {outcome.events, decider.store} do
        {[], _} -> {:ok, state}
        {_, {store, opts}} -> store.sync(decider.stream_name, state, outcome, opts)
        {_, store} when is_atom(store) -> store.sync(decider.stream_name, state, outcome, [])
      end
    end)
  end

  defp set_state(%__MODULE__{} = decider, new_state), do: %{decider | state: new_state}
end
