defmodule Equinox.Decider.Async do
  use GenServer, restart: :transient

  alias Equinox.{Decider, Telemetry}
  alias Equinox.Decider.{Query, Decision, LoadPolicy, LifetimePolicy}

  defmodule Options do
    alias Equinox.Decider.LifetimePolicy

    @opts NimbleOptions.new!(
            supervisor: [
              type: {:or, [:atom, {:in, [:disabled]}]},
              default: :disabled,
              doc: "Name of the DynamicSupervisor which should parent the decider process"
            ],
            registry: [
              type:
                {:or,
                 [
                   :atom,
                   {:in, [:global]},
                   {:tuple, [{:in, [:global]}, :string]},
                   {:in, [:disabled]}
                 ]},
              default: :disabled,
              doc: "Name of the Registry (or :global) under which our decider should be listed"
            ],
            lifetime: [
              type: {:struct, LifetimePolicy},
              default: LifetimePolicy.default(),
              doc: "Decider server lifetime policy"
            ]
          )

    @type t :: [o]
    @type o :: unquote(NimbleOptions.option_typespec(@opts))

    def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
    def docs, do: NimbleOptions.docs(@opts)
    def keys, do: Keyword.keys(@opts.schema)
  end

  defmodule AsyncError do
    defexception [:message]
    @type t :: %__MODULE__{message: String.t()}
  end

  @enforce_keys [:decider, :supervisor, :lifetime]
  defstruct [:server, :decider, :supervisor, :lifetime]

  @type t :: %__MODULE__{
          server: server(),
          decider: Decider.t(),
          supervisor: supervisor(),
          lifetime: LifetimePolicy.t()
        }
  @type server ::
          nil
          | pid()
          | {:global, String.t()}
          | {:via, Registry, {module(), String.t()}}
  @type supervisor ::
          :disabled
          | module()

  @spec wrap_decider(Decider.t(), Options.t()) :: t()
  def wrap_decider(%Decider{} = decider, opts) do
    {registry, opts} = opts |> Options.validate!() |> Keyword.pop(:registry)
    async = struct(__MODULE__, [{:decider, decider} | opts])

    server =
      case registry do
        :disabled -> nil
        :global -> {:global, decider.stream}
        {:global, prefix} -> {:global, prefix <> decider.stream}
        module -> {:via, Registry, {module, decider.stream}}
      end

    %{async | server: server}
  end

  @spec start(t()) :: t()
  def start(%__MODULE__{} = async) do
    with {:ok, pid} <- start_server(async) do
      update_in(async.server, fn
        nil -> pid
        val -> val
      end)
    else
      {:error, {:already_started, _pid}} -> async
      {:error, error} when is_exception(error) -> raise AsyncError, Exception.message(error)
      {:error, error} -> raise AsyncError, "Failed to start process: #{inspect(error)}"
    end
  end

  @spec start_server(t()) :: GenServer.on_start() | DynamicSupervisor.on_start_child()
  def start_server(%__MODULE__{} = async) do
    case async.supervisor do
      :disabled -> start_link(async)
      supervisor -> start_supervised(async, supervisor)
    end
  end

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = async) do
    case async.server do
      pid when is_pid(pid) -> raise AsyncError, "Process #{inspect(pid)} already started"
      name -> GenServer.start_link(__MODULE__, async, name: name)
    end
  end

  @spec start_supervised(t(), GenServer.server()) :: DynamicSupervisor.on_start_child()
  def start_supervised(%__MODULE__{} = async, supervisor) do
    DynamicSupervisor.start_child(supervisor, {__MODULE__, async})
  end

  @spec query(t(), Query.t(), nil | LoadPolicy.t(), timeout()) :: Query.result()
  def query(async, query, load_policy \\ nil, operation_timeout \\ :timer.seconds(5)) do
    ensure_async_started(async, fn async ->
      GenServer.call(async.server, {:query, query, load_policy}, operation_timeout)
    end)
  end

  @spec transact(t(), Decision.without_result(), nil | LoadPolicy.t(), timeout()) ::
          :ok
          | {:error, Decision.Error.t()}
  @spec transact(t(), Decision.with_result(), nil | LoadPolicy.t(), timeout()) ::
          {:ok, Decision.result()}
          | {:error, Decision.Error.t()}
  def transact(async, decision, load_policy \\ nil, operation_timeout \\ :timer.seconds(5)) do
    ensure_async_started(async, fn async ->
      GenServer.call(async.server, {:transact, decision, load_policy}, operation_timeout)
    end)
  end

  defp ensure_async_started(%__MODULE__{server: nil} = async, fun) do
    async |> start() |> fun.()
  end

  defp ensure_async_started(%__MODULE__{server: pid} = async, fun) when is_pid(pid) do
    if Process.alive?(pid) do
      fun.(async)
    else
      ensure_async_started(%{async | server: nil}, fun)
    end
  end

  defp ensure_async_started(%__MODULE__{} = async, fun) do
    try do
      fun.(async)
    catch
      :exit, {:noproc, _} -> async |> start() |> fun.()
    end
  end

  @impl GenServer
  def init(%__MODULE__{} = async) do
    init_time = System.monotonic_time()
    state = async |> Map.from_struct() |> Map.put(:init_time, init_time)
    Telemetry.async_server_init(state)
    {:ok, state, state.lifetime.after_init}
  end

  @impl GenServer
  def handle_call({:query, query, load}, _from, %{decider: decider} = state) do
    {:reply, Decider.query(decider, query, load), state, state.lifetime.after_query}
  end

  @impl GenServer
  def handle_call({:transact, decision, load}, _from, %{decider: decider} = state) do
    {:reply, Decider.transact(decider, decision, load), state, state.lifetime.after_transact}
  end

  @impl GenServer
  def handle_info(:timeout, state) do
    Telemetry.async_server_shutdown(state, :timeout)
    {:stop, :normal, state}
  end
end
