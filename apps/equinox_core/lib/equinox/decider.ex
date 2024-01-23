defmodule Equinox.Decider do
  alias Equinox.{Store, Telemetry}
  alias Equinox.Decider.{Decision, Query, LoadPolicy, ResyncPolicy, Async}

  defmodule Options do
    alias Equinox.Decider.ResyncPolicy

    @opts NimbleOptions.new!(
            store: [
              type: {:or, [:any, {:tuple, [:atom, :keyword_list]}]},
              required: true,
              doc:
                "Implementation of `Equinox.Store` protocol or module and options producing one"
            ],
            load: [
              type:
                {:or,
                 [
                   {:in, [:require_load, :require_leader, :any_cached_value, :assume_empty]},
                   {:tuple, [{:in, [:allow_stale]}, :pos_integer]},
                   {:struct, LoadPolicy}
                 ]},
              default: LoadPolicy.default(),
              doc:
                "Load policy used to define policy for loading the aggregate state before querying / transacting"
            ],
            resync: [
              type:
                {:or,
                 [
                   {:tuple, [{:in, [:max_attempts]}, :non_neg_integer]},
                   {:struct, ResyncPolicy}
                 ]},
              default: ResyncPolicy.default(),
              doc:
                "Retry / Attempts policy used to define policy for retrying based on the conflicting state when there's an Append conflict"
            ]
          )

    @type t :: [o()]
    @type o :: unquote(NimbleOptions.option_typespec(@opts))

    def docs, do: NimbleOptions.docs(@opts)
    def keys, do: Keyword.keys(@opts.schema)

    def validate!(opts) do
      opts
      |> NimbleOptions.validate!(@opts)
      |> Keyword.update!(:store, &init_store/1)
      |> Keyword.update!(:load, &LoadPolicy.wrap/1)
      |> Keyword.update!(:resync, &ResyncPolicy.wrap/1)
    end

    defp init_store({m, o}), do: apply(m, :new, [o])
    defp init_store(not_new), do: not_new
  end

  @enforce_keys [:stream, :store, :load, :resync]
  defstruct [:stream, :store, :load, :resync]

  @type t :: %__MODULE__{
          stream: Store.stream_name(),
          store: Store.t(),
          load: LoadPolicy.t(),
          resync: ResyncPolicy.t()
        }

  @spec for_stream(String.t(), Options.t()) :: t()
  def for_stream(stream_name, opts) do
    opts
    |> Options.validate!()
    |> Keyword.put(:stream, stream_name)
    |> then(&struct(__MODULE__, &1))
  end

  @spec async(t(), Async.Options.t()) :: Async.t()
  @spec async(String.t(), [Options.o() | Async.Options.o()]) :: Async.t()
  def async(stream_name_or_decider, opts \\ [])

  def async(%__MODULE__{} = decider, opts), do: Async.wrap_decider(decider, opts)

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

  @spec query(t() | Async.t(), Query.t(), nil | LoadPolicy.t()) :: Query.result()
  def query(decider, query, load_policy \\ nil)

  def query(%Async{} = async, query, load_policy) do
    Async.query(async, query, load_policy)
  end

  def query(%__MODULE__{} = decider, query, load_policy) do
    Telemetry.span_decider_query(decider, query, load_policy, fn ->
      with {:ok, state} <- load_state(decider, LoadPolicy.wrap(load_policy || decider.load)) do
        execute_query(decider, state, query)
      else
        {:error, unrecoverable_error} -> raise unrecoverable_error
      end
    end)
  end

  @spec transact(t() | Async.t(), Decision.without_result(), nil | LoadPolicy.t()) ::
          :ok
          | {:error, Decision.Error.t()}
  @spec transact(t() | Async.t(), Decision.with_result(), nil | LoadPolicy.t()) ::
          {:ok, Decision.result()}
          | {:error, Decision.Error.t()}
  def transact(decider, decision, load_policy \\ nil)

  def transact(%Async{} = async, decision, load_policy) do
    Async.transact(async, decision, load_policy)
  end

  def transact(%__MODULE__{} = decider, decision, load_policy) do
    Telemetry.span_decider_transact(decider, decision, load_policy, fn ->
      with {:ok, state} <- load_state(decider, LoadPolicy.wrap(load_policy || decider.load)),
           {:ok, result} <- transact_with_resync(decider, state, decision) do
        result
      else
        {:error, %Decision.Error{} = decision_error} -> {:error, decision_error}
        {:error, unrecoverable_error} -> raise unrecoverable_error
      end
    end)
  end

  defp load_state(%__MODULE__{} = decider, policy) do
    Telemetry.span_decider_load(decider, policy, fn ->
      Store.load(decider.store, decider.stream, policy)
    end)
  end

  defp execute_query(%__MODULE__{} = decider, state, query) do
    Telemetry.span_decider_query_execute(decider, state, query, fn ->
      Query.execute(query, state.value)
    end)
  end

  defp transact_with_resync(%__MODULE__{} = decider, state, decision, attempt \\ 0) do
    case attempt_to_transact(decider, state, decision, attempt) do
      {:ok, result} ->
        {:ok, result}

      {:error, error} ->
        {:error, error}

      {:conflict, resync_fun} ->
        with :ok <- ResyncPolicy.validate_attempt(decider.resync, attempt),
             {:ok, resynced_state} <- resync_fun.() do
          transact_with_resync(decider, resynced_state, decision, attempt + 1)
        end
    end
  end

  defp attempt_to_transact(%__MODULE__{} = decider, state, decision, attempt) do
    Telemetry.span_decider_transact_attempt(decider, state, decision, attempt, fn ->
      with {:ok, result, events} <- execute_decision(decider, state, decision),
           {:ok, _synced_state} <- sync_state(decider, state, events) do
        {:ok, result}
      end
    end)
  end

  defp execute_decision(%__MODULE__{} = decider, state, decision) do
    Telemetry.span_decider_transact_decision(decider, state, decision, fn ->
      Decision.execute(decision, state.value)
    end)
  end

  defp sync_state(%__MODULE__{} = decider, state, events) do
    unless Store.EventsToSync.empty?(events) do
      Telemetry.span_decider_sync(decider, state, events, fn ->
        Store.sync(decider.store, decider.stream, state, events)
      end)
    else
      {:ok, state}
    end
  end
end
