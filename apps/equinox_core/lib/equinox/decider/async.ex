defmodule Equinox.Decider.Async do
  use GenServer, restart: :transient

  alias Equinox.{Decider, Store, Telemetry}
  alias Equinox.Decider.{Query, Decision, LoadPolicy, LifetimePolicy}
  alias Equinox.Decider.Async.Options

  @enforce_keys [:decider, :supervisor, :lifetime, :context]
  defstruct [:server, :decider, :supervisor, :lifetime, :context]

  @type t :: %__MODULE__{
          server: server(),
          decider: Decider.t(),
          supervisor: supervisor(),
          lifetime: LifetimePolicy.t(),
          context: Store.EventsToSync.context()
        }
  @type server ::
          nil
          | pid()
          | {:global, String.t()}
          | {:via, Registry, {module(), String.t()}}
  @type supervisor ::
          :disabled
          | module()

  defmodule AsyncError do
    defexception [:message]
    @type t :: %__MODULE__{message: String.t()}
  end

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

  @spec query(t(), Query.t(), LoadPolicy.t(), timeout()) :: term()
  def query(async, query, load \\ LoadPolicy.default(), timeout \\ :timer.seconds(5)) do
    ensure_async_started(async, fn async ->
      GenServer.call(async.server, {:query, query, load}, timeout)
    end)
  end

  @spec transact(t(), Decision.without_result(), LoadPolicy.t(), timeout()) ::
          :ok | {:error, term()}
  @spec transact(t(), Decision.with_result(), LoadPolicy.t(), timeout()) ::
          {:ok, term()} | {:error, term()}
  def transact(async, decision, load \\ LoadPolicy.default(), timeout \\ :timer.seconds(5)) do
    ensure_async_started(async, fn async ->
      GenServer.call(async.server, {:transact, decision, async.context, load}, timeout)
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
    Telemetry.async_server_init(async)
    {:ok, async, async.lifetime.after_init}
  end

  @impl GenServer
  def handle_call({:query, query, load}, _from, async) do
    {:reply, Decider.query(async.decider, query, load), async, async.lifetime.after_query}
  end

  @impl GenServer
  def handle_call({:transact, decision, context, load}, _from, async) do
    decider = %{async.decider | context: context}
    {:reply, Decider.transact(decider, decision, load), async, async.lifetime.after_transact}
  end

  @impl GenServer
  def handle_info(:timeout, server_state) do
    Telemetry.async_server_stop(server_state, :timeout)
    {:stop, :normal, server_state}
  end
end
