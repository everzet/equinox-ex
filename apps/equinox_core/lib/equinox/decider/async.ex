defmodule Equinox.Decider.Async do
  use GenServer, restart: :transient

  alias Equinox.{Decider, Store, Lifetime, Telemetry}
  alias Equinox.Decider.{Query, Decision}
  alias Equinox.Decider.Async.Options

  @enforce_keys [:supervisor, :registry, :lifetime]
  defstruct [:server_name, :decider, :supervisor, :registry, :lifetime, :context]

  @type t :: %__MODULE__{
          decider: Decider.t(),
          lifetime: Lifetime.t(),
          registry: registry(),
          supervisor: supervisor(),
          server_name: server_name(),
          context: Store.sync_context()
        }
  @type server_name ::
          nil | pid() | {:global, String.t()} | {:via, Registry, {module(), String.t()}}
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

  @spec start(t()) :: t()
  def start(%__MODULE__{} = async) do
    with {:ok, pid} <- start_server(async) do
      update_in(async.server_name, fn
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
    case async.server_name do
      pid when is_pid(pid) -> raise AsyncError, "Process #{inspect(pid)} already started"
      name -> GenServer.start_link(__MODULE__, async, name: name)
    end
  end

  @spec start_supervised(t(), GenServer.server()) :: DynamicSupervisor.on_start_child()
  def start_supervised(%__MODULE__{} = async, supervisor) do
    DynamicSupervisor.start_child(supervisor, {__MODULE__, async})
  end

  @spec query(t(), Query.t()) :: {any(), t()}
  def query(async, query) do
    ensure_async_started(async, fn async ->
      {GenServer.call(async.server_name, {:query, query}), async}
    end)
  end

  @spec transact(t(), Decision.without_result()) ::
          {:ok, t()} | {:error, term(), t()}
  @spec transact(t(), Decision.with_result()) ::
          {:ok, term(), t()} | {:error, term(), t()}
  def transact(async, decision) do
    ensure_async_started(async, fn async ->
      case GenServer.call(async.server_name, {:transact, decision, async.context}) do
        :ok -> {:ok, async}
        {:ok, result} -> {:ok, result, async}
        {:error, error} -> {:error, error, async}
      end
    end)
  end

  defp ensure_async_started(%__MODULE__{server_name: nil} = async, fun) do
    async |> start() |> fun.()
  end

  defp ensure_async_started(%__MODULE__{server_name: pid} = async, fun) when is_pid(pid) do
    if not Process.alive?(pid) do
      ensure_async_started(%{async | server_name: nil}, fun)
    else
      fun.(async)
    end
  end

  defp ensure_async_started(%__MODULE__{} = async, fun) do
    fun.(async)
  catch
    :exit, {:noproc, _} -> async |> start() |> fun.()
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
      Telemetry.span_async_transact(server_state, context, decision, fn ->
        Decider.transact(%{server_state.decider | context: context}, decision)
      end)

    case transact_result do
      {:ok, updated_decider} ->
        {:reply, :ok, %{server_state | decider: updated_decider},
         server_state.settings.lifetime.after_transact(updated_decider.state.value)}

      {:ok, result, updated_decider} ->
        {:reply, {:ok, result}, %{server_state | decider: updated_decider},
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
