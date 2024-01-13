defmodule Equinox.Decider.Async do
  use GenServer, restart: :transient

  alias Equinox.{Decider, Lifetime, Telemetry}
  alias Equinox.Decider.{Query, Decision}
  alias Equinox.Decider.Async.Options

  @enforce_keys [:supervisor, :registry, :lifetime]
  defstruct [:server_name, :decider, :supervisor, :registry, :lifetime]

  @type t :: %__MODULE__{
          decider: Decider.t(),
          lifetime: Lifetime.t(),
          registry: registry(),
          supervisor: supervisor(),
          server_name: server_name()
        }
  @type server_name :: nil | {:global, String.t()} | {:via, Registry, {module(), String.t()}}
  @type registry :: :disabled | :global | {:global, prefix :: String.t()} | module()
  @type supervisor :: :disabled | module()

  defmodule AsyncError do
    defexception [:message]
    @type t :: %__MODULE__{message: String.t()}
  end

  @spec wrap_decider(Decider.t(), Options.t()) :: t()
  def wrap_decider(%Decider{} = decider, opts) do
    opts = Options.validate!(opts)
    async = struct(__MODULE__, [{:decider, decider} | opts])

    server_name =
      case async.registry do
        :disabled -> nil
        :global -> {:global, decider.stream_name}
        {:global, prefix} -> {:global, prefix <> decider.stream_name}
        module -> {:via, Registry, {module, decider.stream_name}}
      end

    %{async | server_name: server_name}
  end

  @spec start(t()) :: t() | pid()
  def start(%__MODULE__{} = async) do
    with {:ok, pid} <- start_server(async) do
      if(async.server_name, do: async, else: pid)
    else
      {:error, {:already_started, _pid}} -> async
      {:error, error} -> raise AsyncError, inspect(error)
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
    GenServer.start_link(__MODULE__, async, name: async.server_name)
  end

  @spec start_supervised(t(), GenServer.server()) :: DynamicSupervisor.on_start_child()
  def start_supervised(%__MODULE__{} = async, supervisor) do
    DynamicSupervisor.start_child(supervisor, {__MODULE__, async})
  end

  @spec query(t(), Query.t()) :: {any(), t()}
  @spec query(pid(), Query.t()) :: {any(), pid()}
  def query(async_or_pid, query) do
    ensure_async_started(async_or_pid, fn server_name_or_pid ->
      {GenServer.call(server_name_or_pid, {:query, query}), async_or_pid}
    end)
  end

  @spec transact(t(), Decision.t(), Decider.context()) :: {:ok, t()} | {:error, term(), t()}
  @spec transact(pid(), Decision.t(), Decider.context()) :: {:ok, pid()} | {:error, term(), pid()}
  def transact(async_or_pid, decision, context \\ %{}) do
    ensure_async_started(async_or_pid, fn server_name_or_pid ->
      case GenServer.call(server_name_or_pid, {:transact, decision, context}) do
        :ok -> {:ok, async_or_pid}
        {:error, error} -> {:error, error, async_or_pid}
      end
    end)
  end

  defp ensure_async_started(pid, fun) when is_pid(pid) do
    if not Process.alive?(pid) do
      raise AsyncError, "given process #{inspect(pid)} is not alive"
    else
      fun.(pid)
    end
  end

  defp ensure_async_started(%__MODULE__{server_name: nil}, _fun) do
    raise AsyncError, "failed to auto-start decider. Start manually or provide `registry` setting"
  end

  defp ensure_async_started(%__MODULE__{server_name: server_name} = server, fun) do
    fun.(server_name)
  catch
    :exit, {:noproc, _} ->
      with {:ok, _pid} <- start_server(server) do
        fun.(server_name)
      end
  end

  @impl GenServer
  def init(%__MODULE__{} = async) do
    {decider, settings} = Map.pop(async, :decider)
    server_state = %{decider: decider, settings: settings}
    Telemetry.async_server_init(server_state)
    {:ok, server_state, {:continue, :load}}
  end

  @impl GenServer
  def handle_continue(:load, %{decider: decider} = server_state) do
    decider =
      Telemetry.span_async_load(server_state, fn ->
        if not Decider.loaded?(decider) do
          Decider.load(decider)
        else
          decider
        end
      end)

    {:noreply, %{server_state | decider: decider},
     server_state.settings.lifetime.after_init(decider.state.value)}
  end

  @impl GenServer
  def handle_call({:query, query}, _from, server_state) do
    {query_result, loaded_decider} =
      Telemetry.span_async_query(server_state, query, fn ->
        Decider.query(server_state.decider, query)
      end)

    {:reply, query_result, %{server_state | decider: loaded_decider},
     server_state.settings.lifetime.after_query(server_state.decider.state.value)}
  end

  @impl GenServer
  def handle_call({:transact, decision, context}, _from, server_state) do
    transact_result =
      Telemetry.span_async_transact(server_state, decision, context, fn ->
        Decider.transact(server_state.decider, decision, context)
      end)

    case transact_result do
      {:ok, updated_decider} ->
        {:reply, :ok, %{server_state | decider: updated_decider},
         server_state.settings.lifetime.after_transact(updated_decider.state.value)}

      {:error, error, loaded_decider} ->
        {:reply, {:error, error}, %{server_state | decider: loaded_decider},
         server_state.settings.lifetime.after_transact(server_state.decider.state.value)}
    end
  end

  @impl GenServer
  def handle_info(:timeout, server_state) do
    Telemetry.async_server_stop(server_state, :timeout)
    {:stop, :normal, server_state}
  end
end
