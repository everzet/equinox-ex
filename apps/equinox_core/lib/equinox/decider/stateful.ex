defmodule Equinox.Decider.Stateful do
  use GenServer, restart: :transient

  alias Equinox.{Decider, Telemetry}
  alias Equinox.Decider.Stateless
  alias Equinox.Decider.Actions.{Query, Decision}

  @enforce_keys [:supervisor, :registry, :lifetime]
  defstruct [:server_name, :stateless, :supervisor, :registry, :lifetime]

  @type t :: %__MODULE__{}

  defmodule Options do
    @opts NimbleOptions.new!(
            supervisor: [
              type: {:custom, __MODULE__, :validate_supervisor, []},
              required: true,
              doc: "Name of the DynamicSupervisor which should parent the decider process"
            ],
            registry: [
              type: {:custom, __MODULE__, :validate_registry, []},
              required: true,
              doc: "Name of the Registry (or :global) under which our decider should be listed"
            ],
            lifetime: [
              type: {:custom, __MODULE__, :validate_lifetime, []},
              required: true,
              doc: "Server lifetime spec module that implements `Equinox.Lifetime` behaviour"
            ]
          )

    @type t :: [o]
    @type o :: unquote(NimbleOptions.option_typespec(@opts))

    def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
    def docs, do: NimbleOptions.docs(@opts)

    def validate_supervisor(supervisor) do
      case supervisor do
        :disabled ->
          {:ok, :disabled}

        supervisor when is_atom(supervisor) ->
          if supervisor |> GenServer.whereis() |> Process.alive?() do
            {:ok, supervisor}
          else
            {:error, "must be a running DynamicSupervisor, but got #{inspect(supervisor)}"}
          end

        _ ->
          {:error,
           "must be a running DynamicSupervisor or :disabled, but got #{inspect(supervisor)}"}
      end
    end

    def validate_registry(registry) do
      case registry do
        :disabled ->
          {:ok, :disabled}

        :global ->
          {:ok, :global}

        {:global, prefix} ->
          {:ok, {:global, prefix}}

        registry when is_atom(registry) ->
          if registry |> GenServer.whereis() |> Process.alive?() do
            {:ok, registry}
          else
            {:error, "must be a running Registry, but got #{inspect(registry)}"}
          end

        _ ->
          {:error,
           "must be a running Registry, :global, {:global, prefix} or :disabled, but got #{inspect(registry)}"}
      end
    end

    def validate_lifetime(lifetime) do
      case lifetime do
        lifetime when is_atom(lifetime) ->
          Code.ensure_loaded(lifetime)

          if function_exported?(lifetime, :after_init, 1) do
            {:ok, lifetime}
          else
            {:error,
             "must be a module implementing `Equinox.Lifetime`, but got #{inspect(lifetime)}"}
          end

        _ ->
          {:error,
           "must be a module implementing `Equinox.Lifetime`, but got #{inspect(lifetime)}"}
      end
    end
  end

  @spec wrap_stateless(Stateless.t(), Options.t()) :: t()
  def wrap_stateless(%Stateless{} = stateless, opts) do
    opts = Options.validate!(opts)
    settings = struct(__MODULE__, [{:stateless, stateless} | opts])

    server_name =
      case settings.registry do
        :disabled -> nil
        :global -> {:global, stateless.stream_name}
        {:global, prefix} -> {:global, prefix <> stateless.stream_name}
        module -> {:via, Registry, {module, stateless.stream_name}}
      end

    %{settings | server_name: server_name}
  end

  @spec start(t()) :: t() | pid()
  def start(%__MODULE__{} = settings) do
    with {:ok, pid} <- start_server(settings) do
      if(settings.server_name, do: settings, else: pid)
    else
      {:error, {:already_started, _pid}} -> settings
      {:error, error} -> raise RuntimeError, message: "Decider.Stateful: #{inspect(error)}"
    end
  end

  @spec start_server(t()) :: GenServer.on_start() | DynamicSupervisor.on_start_child()
  def start_server(%__MODULE__{} = settings) do
    case settings.supervisor do
      :disabled -> start_link(settings)
      supervisor -> start_supervised(settings, supervisor)
    end
  end

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = settings) do
    GenServer.start_link(__MODULE__, settings, name: settings.server_name)
  end

  @spec start_supervised(t(), GenServer.server()) :: DynamicSupervisor.on_start_child()
  def start_supervised(%__MODULE__{} = settings, supervisor) do
    DynamicSupervisor.start_child(supervisor, {__MODULE__, settings})
  end

  @spec query(t() | pid(), Query.t()) :: any()
  def query(settings_or_pid, query) do
    ensure_server_started(settings_or_pid, fn server_name_or_pid ->
      GenServer.call(server_name_or_pid, {:query, query})
    end)
  end

  @spec transact(t(), Decision.t(), Decider.context()) :: {:ok, t()} | {:error, term()}
  @spec transact(pid(), Decision.t(), Decider.context()) :: {:ok, pid()} | {:error, term()}
  def transact(settings_or_pid, decision, context \\ %{}) do
    ensure_server_started(settings_or_pid, fn server_name_or_pid ->
      case GenServer.call(server_name_or_pid, {:transact, decision, context}) do
        :ok -> {:ok, settings_or_pid}
        {:error, error} -> {:error, error}
      end
    end)
  end

  defp ensure_server_started(pid, fun) when is_pid(pid) do
    if not Process.alive?(pid) do
      raise RuntimeError,
        message: "Decider.Stateful: Given process #{inspect(pid)} is not alive"
    else
      fun.(pid)
    end
  end

  defp ensure_server_started(%__MODULE__{server_name: nil}, _fun) do
    raise RuntimeError,
      message: "Decider.Stateful failed to start: Start manually or provide `registry` setting"
  end

  defp ensure_server_started(%__MODULE__{server_name: server_name} = server, fun) do
    fun.(server_name)
  catch
    :exit, {:noproc, _} ->
      with {:ok, _pid} <- start_server(server) do
        fun.(server_name)
      end
  end

  @impl GenServer
  def init(%__MODULE__{} = settings) do
    {decider, settings} = Map.pop(settings, :stateless)
    server = %{decider: decider, settings: settings}
    Telemetry.decider_server_init(server)
    {:ok, server, {:continue, :load}}
  end

  @impl GenServer
  def handle_continue(:load, %{decider: decider} = server) do
    decider =
      Telemetry.span_decider_server_load(server, fn ->
        if not Stateless.loaded?(decider) do
          Stateless.load(decider)
        else
          decider
        end
      end)

    {:noreply, %{server | decider: decider},
     server.settings.lifetime.after_init(decider.state.value)}
  end

  @impl GenServer
  def handle_call({:query, query}, _from, server) do
    query_result =
      Telemetry.span_decider_server_query(server, query, fn ->
        Stateless.query(server.decider, query)
      end)

    {:reply, query_result, server,
     server.settings.lifetime.after_query(server.decider.state.value)}
  end

  @impl GenServer
  def handle_call({:transact, decision, context}, _from, server) do
    transact_result =
      Telemetry.span_decider_server_transact(server, decision, context, fn ->
        Stateless.transact(server.decider, decision, context)
      end)

    case transact_result do
      {:ok, updated_decider} ->
        {:reply, :ok, %{server | decider: updated_decider},
         server.settings.lifetime.after_transact(updated_decider.state.value)}

      {:error, error} ->
        {:reply, {:error, error}, server,
         server.settings.lifetime.after_transact(server.decider.state.value)}
    end
  end

  @impl GenServer
  def handle_info(:timeout, server) do
    Telemetry.decider_server_stop(server, :timeout)
    {:stop, :normal, server}
  end
end
