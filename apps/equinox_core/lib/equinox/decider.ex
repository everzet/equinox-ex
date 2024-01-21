defmodule Equinox.Decider do
  alias Equinox.{Store, Store.EventsToSync}
  alias Equinox.Decider.{Decision, Query, LoadPolicy, ResyncPolicy, Async}

  @enforce_keys [:stream, :store, :load, :resync]
  defstruct [:stream, :store, :load, :resync]

  @type t :: %__MODULE__{
          stream: Store.stream_name(),
          store: Store.t(),
          load: LoadPolicy.t(),
          resync: ResyncPolicy.t()
        }

  defmodule Options do
    alias Equinox.Decider.ResyncPolicy

    @opts NimbleOptions.new!(
            store: [
              type: :any,
              required: true,
              doc: "An implementation of `Equinox.Store` protocol"
            ],
            load: [
              type: {:struct, LoadPolicy},
              default: LoadPolicy.default(),
              doc:
                "Load policy used to define policy for loading the aggregate state before querying / transacting"
            ],
            resync: [
              type: {:struct, ResyncPolicy},
              default: ResyncPolicy.default(),
              doc:
                "Retry / Attempts policy used to define policy for retrying based on the conflicting state when there's an Append conflict"
            ]
          )

    @type t :: [o()]
    @type o :: unquote(NimbleOptions.option_typespec(@opts))

    def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
    def docs, do: NimbleOptions.docs(@opts)
    def keys, do: Keyword.keys(@opts.schema)
  end

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

  @spec query(Async.t(), Query.t(), nil | LoadPolicy.t()) :: term()
  @spec query(t(), Query.t(), nil | LoadPolicy.t()) :: term()
  def query(decider, query, load \\ nil)

  def query(%Async{} = async, query, load), do: Async.query(async, query, load)

  def query(%__MODULE__{} = decider, query, load) do
    with {:ok, state} <- load_state(decider, load || decider.load) do
      Query.execute(query, state.value)
    else
      {:error, unrecoverable_error} -> raise unrecoverable_error
    end
  end

  @spec transact(Async.t(), Decision.without_result(), nil | LoadPolicy.t()) ::
          :ok | {:error, term()}
  @spec transact(Async.t(), Decision.with_result(), nil | LoadPolicy.t()) ::
          {:ok, term()} | {:error, term()}
  @spec transact(t(), Decision.without_result(), nil | LoadPolicy.t()) ::
          :ok | {:error, term()}
  @spec transact(t(), Decision.with_result(), nil | LoadPolicy.t()) ::
          {:ok, term()} | {:error, term()}
  def transact(decider, decision, load \\ nil)

  def transact(%Async{} = async, decision, load), do: Async.transact(async, decision, load)

  def transact(%__MODULE__{} = decider, decision, load) do
    with {:ok, state} <- load_state(decider, load || decider.load),
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
    with {:ok, result, events} <- Decision.execute(decision, state.value),
         {:ok, _synced_state} <- sync_state(decider, state, events) do
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

  defp sync_state(%__MODULE__{} = decider, state, events) do
    if not EventsToSync.empty?(events) do
      Store.sync(decider.store, decider.stream, state, events)
    else
      {:ok, state}
    end
  end
end
