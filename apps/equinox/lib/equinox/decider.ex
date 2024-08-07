defmodule Equinox.Decider do
  alias Equinox.{Store, Telemetry}
  alias Equinox.Codec.StreamName
  alias Equinox.Decider.{Decision, Query, LoadPolicy, ResyncPolicy, Async}

  defmodule Options do
    alias Equinox.Decider.ResyncPolicy

    @opts NimbleOptions.new!(
            store: [
              type: {:or, [{:tuple, [:atom, :keyword_list]}, :mfa]},
              required: true,
              doc: "Builder function returning implementation of `Equinox.Store` protocol"
            ],
            load: [
              type:
                {:or,
                 [
                   {:struct, LoadPolicy},
                   {:in,
                    [:default, :assume_empty, :require_load, :require_leader, :any_cached_value]},
                   {:non_empty_keyword_list,
                    max_cache_age: [type: :non_neg_integer],
                    requires_leader?: [type: :boolean],
                    assumes_empty?: [type: :boolean]}
                 ]},
              default: :default,
              doc: "Default aggregate state loading policy for querying and transacting"
            ],
            resync: [
              type:
                {:or,
                 [
                   {:struct, ResyncPolicy},
                   {:in, [:default]},
                   {:non_empty_keyword_list, max_attempts: [type: :non_neg_integer]}
                 ]},
              default: :default,
              doc: "Aggregate resync policy in case of state <-> stream version conflicts"
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
      |> Keyword.update!(:load, &LoadPolicy.new/1)
      |> Keyword.update!(:resync, &ResyncPolicy.new/1)
    end

    defp init_store({m, o}), do: m.new(o)
    defp init_store({m, f, a}), do: apply(m, f, a)
  end

  @enforce_keys [:stream, :store, :load, :resync]
  defstruct [:stream, :store, :load, :resync]

  @type t :: %__MODULE__{
          stream: StreamName.t(),
          store: Store.t(),
          load: LoadPolicy.t(),
          resync: ResyncPolicy.t()
        }

  @spec for_stream(StreamName.t(), Options.t()) :: t()
  def for_stream(%StreamName{} = stream_name, opts) do
    opts
    |> Options.validate!()
    |> Keyword.put(:stream, stream_name)
    |> then(&struct(__MODULE__, &1))
  end

  @spec async(t(), Async.Options.t()) :: Async.t()
  @spec async(StreamName.t(), [Options.o() | Async.Options.o()]) :: Async.t()
  def async(stream_or_decider, opts \\ [])

  def async(%__MODULE__{} = decider, opts), do: Async.wrap_decider(decider, opts)

  def async(%StreamName{} = stream_name, opts) do
    {decider_opts, async_opts} = Keyword.split(opts, Options.keys())

    stream_name
    |> for_stream(decider_opts)
    |> async(async_opts)
  end

  @spec query(t() | Async.t(), Query.t(), nil | LoadPolicy.option()) :: Query.result()
  def query(decider, query, load_policy \\ nil)

  def query(%Async{} = async, query, load_policy) do
    Async.query(async, query, load_policy)
  end

  def query(%__MODULE__{} = decider, query, load_policy) do
    Telemetry.span_decider_query(decider, query, load_policy, fn ->
      case load_state(decider, load_policy) do
        {:ok, state} -> execute_query(decider, state, query)
        {:error, error} -> raise error
      end
    end)
  end

  @spec transact(t() | Async.t(), Decision.without_result(), nil | LoadPolicy.option()) ::
          :ok
          | {:error, Decision.Error.t()}
  @spec transact(t() | Async.t(), Decision.with_result(), nil | LoadPolicy.option()) ::
          {:ok, Decision.result()}
          | {:error, Decision.Error.t()}
  def transact(decider, decision, load_policy \\ nil)

  def transact(%Async{} = async, decision, load_policy) do
    Async.transact(async, decision, load_policy)
  end

  def transact(%__MODULE__{} = decider, decision, load_policy) do
    Telemetry.span_decider_transact(decider, decision, load_policy, fn ->
      with {:ok, state} <- load_state(decider, load_policy),
           {:ok, result} <- transact_with_resync(decider, state, decision) do
        result
      else
        {:error, %Decision.Error{} = decision_error} -> {:error, decision_error}
        {:error, unrecoverable_error} -> raise unrecoverable_error
      end
    end)
  end

  defp load_state(%__MODULE__{} = decider, load_policy) do
    Telemetry.span_decider_load(decider, load_policy, fn ->
      Store.load(decider.store, decider.stream, LoadPolicy.new(load_policy || decider.load))
    end)
  end

  defp execute_query(%__MODULE__{} = decider, state, query) do
    Telemetry.span_decider_query_execute(decider, state, query, fn ->
      Query.execute(query, state.value)
    end)
  end

  defp transact_with_resync(%__MODULE__{} = decider, state, decision, attempt \\ 1) do
    case attempt_to_transact(decider, state, decision, attempt) do
      {:ok, result} ->
        {:ok, result}

      {:error, error} ->
        {:error, error}

      {:conflict, resync} ->
        with {:ok, resynced_state} <- resync_state(decider, resync, attempt) do
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

  defp resync_state(%__MODULE__{} = decider, resync, attempt) do
    Telemetry.span_decider_transact_resync(decider, resync, attempt, fn ->
      with :ok <- ResyncPolicy.validate_resync_attempt(decider.resync, attempt) do
        resync.()
      end
    end)
  end

  defp execute_decision(%__MODULE__{} = decider, state, decision) do
    Telemetry.span_decider_transact_decision(decider, state, decision, fn ->
      Decision.execute(decision, state.value)
    end)
  end

  defp sync_state(%__MODULE__{} = decider, state, events) do
    if Store.EventsToSync.empty?(events) do
      {:ok, state}
    else
      Telemetry.span_decider_sync(decider, state, events, fn ->
        Store.sync(decider.store, decider.stream, state, events)
      end)
    end
  end
end
