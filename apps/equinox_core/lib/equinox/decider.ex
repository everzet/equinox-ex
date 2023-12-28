defmodule Equinox.Decider do
  defmodule Query do
    alias Equinox.State

    @type t :: (State.value() -> any())

    @spec execute(t(), State.t()) :: any()
    def execute(query, %State{value: value}), do: query.(value)
  end

  defmodule Decision do
    alias Equinox.State
    alias Equinox.Events.DomainEvent

    @type t ::
            (State.value() ->
               nil
               | DomainEvent.t()
               | list(DomainEvent.t())
               | {:ok, DomainEvent.t() | list(DomainEvent.t())}
               | {:error, term()})

    @spec execute(t(), State.t()) :: {:ok, list(DomainEvent.t())} | {:error, term()}
    def execute(decision, %State{value: value}) do
      case decision.(value) do
        {:error, error} -> {:error, error}
        {:ok, event_or_events} -> {:ok, List.wrap(event_or_events)}
        nil_or_event_or_events -> {:ok, List.wrap(nil_or_event_or_events)}
      end
    end
  end

  defmodule Stateless do
    alias Equinox.Stream.StreamName
    alias Equinox.{State, Store, Codec, Fold}
    alias Equinox.Decider.{Query, Decision}

    @enforce_keys [:stream_name, :store, :codec, :fold]
    defstruct [
      :stream_name,
      :state,
      :store,
      :codec,
      :fold,
      :max_load_attempts,
      :max_sync_attempts,
      :max_resync_attempts
    ]

    @type t :: %__MODULE__{}

    @opts NimbleOptions.new!(
            store: [
              type: :atom,
              required: true,
              doc: "Persistence module that implements `Equinox.Store` behaviour"
            ],
            codec: [
              type: :atom,
              required: true,
              doc: "Event (en|de)coding module that implements `Equinox.Codec` behaviour"
            ],
            fold: [
              type: :atom,
              required: true,
              doc: "State generation module that implements `Equinox.Fold` behaviour"
            ],
            max_load_attempts: [
              type: :pos_integer,
              default: 2,
              doc: "How many times (in total) should we try to load the state on load errors"
            ],
            max_sync_attempts: [
              type: :pos_integer,
              default: 2,
              doc: "How many times (in total) should we try to sync the state on write errors"
            ],
            max_resync_attempts: [
              type: :non_neg_integer,
              default: 1,
              doc: "How many times should we try to resync the state on version conflict"
            ]
          )

    @type option :: unquote(NimbleOptions.option_typespec(@opts))
    @spec for_stream(StreamName.t() | String.t(), [option]) :: t()
    def for_stream(stream_name, opts) do
      case NimbleOptions.validate(opts, @opts) do
        {:ok, opts} ->
          decider =
            opts
            |> Keyword.put(:stream_name, String.Chars.to_string(stream_name))
            |> then(&struct(__MODULE__, &1))

          %{decider | state: State.init(decider.fold)}

        {:error, validation_error} ->
          raise ArgumentError, message: Exception.message(validation_error)
      end
    end

    @spec load(t()) :: t()
    def load(%__MODULE__{} = decider) do
      load_state_with_retry(decider)
    end

    @spec query(t(), Query.t()) :: any()
    def query(%__MODULE__{} = decider, query_fun) do
      Query.execute(query_fun, decider.state)
    end

    @spec transact(t(), Decision.t(), Codec.ctx()) :: {:ok, t()} | {:error, term()}
    def transact(%__MODULE__{} = decider, decision, ctx \\ nil) do
      transact_with_resync(decider, decision, ctx)
    end

    defp transact_with_resync(%__MODULE__{} = decider, decision, ctx, resync_attempt \\ 0) do
      case Decision.execute(decision, decider.state) do
        {:error, error} ->
          {:error, error}

        {:ok, []} ->
          {:ok, decider}

        {:ok, events} ->
          try do
            {:ok, sync_state_with_retry(decider, ctx, events)}
          rescue
            version_conflict in [Store.StreamVersionConflict] ->
              if resync_attempt < decider.max_resync_attempts do
                decider
                |> load_state_with_retry()
                |> transact_with_resync(decision, ctx, resync_attempt + 1)
              else
                reraise version_conflict, __STACKTRACE__
              end
          end
      end
    end

    defp sync_state_with_retry(%__MODULE__{} = decider, ctx, events, sync_attempt \\ 1) do
      try do
        new_state =
          decider.stream_name
          |> decider.store.sync!(decider.state, events, ctx, decider.codec, decider.fold)

        %{decider | state: new_state}
      rescue
        unrecoverable in [Store.StreamVersionConflict, Codec.CodecError, Fold.FoldError] ->
          reraise unrecoverable, __STACKTRACE__

        recoverable ->
          if sync_attempt < decider.max_sync_attempts do
            sync_state_with_retry(decider, ctx, events, sync_attempt + 1)
          else
            reraise recoverable, __STACKTRACE__
          end
      end
    end

    defp load_state_with_retry(%__MODULE__{} = decider, load_attempt \\ 1) do
      try do
        new_state =
          decider.stream_name
          |> decider.store.load!(decider.state, decider.codec, decider.fold)

        %{decider | state: new_state}
      rescue
        unrecoverable in [Codec.CodecError, Fold.FoldError] ->
          reraise unrecoverable, __STACKTRACE__

        recoverable ->
          if load_attempt < decider.max_load_attempts do
            load_state_with_retry(decider, load_attempt + 1)
          else
            reraise recoverable, __STACKTRACE__
          end
      end
    end
  end

  defmodule Stateful do
    use GenServer, restart: :transient

    alias Equinox.Stream.StreamName
    alias Equinox.Decider.Stateless
    alias Equinox.Codec

    @enforce_keys [:stream_name, :supervisor, :registry, :lifetime, :store, :codec, :fold]
    defstruct [
      :stream_name,
      :server_name,
      :supervisor,
      :registry,
      :lifetime,
      :store,
      :codec,
      :fold,
      :on_init,
      :max_load_attempts,
      :max_sync_attempts,
      :max_resync_attempts
    ]

    @type t :: %__MODULE__{}

    @opts NimbleOptions.new!(
            supervisor: [
              type: {:or, [:atom, {:in, [:disabled]}]},
              required: true,
              doc: "Name of the DynamicSupervisor to start decider under"
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
              required: true,
              doc: "Name of the registry under which to register the decider"
            ],
            lifetime: [
              type: :atom,
              required: true,
              doc: "Process lifetime defition module that implements `Equinox.Lifetime` behaviour"
            ],
            store: [
              type: :atom,
              required: true,
              doc: "Persistence module that implements `Equinox.Store` behaviour"
            ],
            codec: [
              type: :atom,
              required: true,
              doc: "Event (en|de)coding module that implements `Equinox.Codec` behaviour"
            ],
            fold: [
              type: :atom,
              required: true,
              doc: "State generation module that implements `Equinox.Fold` behaviour"
            ],
            on_init: [
              type: {:fun, 0},
              doc: "Function to execute inside spawned process just before it is initialized"
            ],
            max_load_attempts: [
              type: :pos_integer,
              default: 2,
              doc: "How many times (in total) should we try to load the state on load errors"
            ],
            max_sync_attempts: [
              type: :pos_integer,
              default: 2,
              doc: "How many times (in total) should we try to sync the state on write errors"
            ],
            max_resync_attempts: [
              type: :non_neg_integer,
              default: 1,
              doc: "How many times should we try to resync the state on version conflict"
            ]
          )

    @type option :: unquote(NimbleOptions.option_typespec(@opts))
    @spec for_stream(StreamName.t() | String.t(), [option()]) :: t()
    def for_stream(stream_name, opts) do
      case NimbleOptions.validate(opts, @opts) do
        {:ok, opts} ->
          decider =
            opts
            |> Keyword.put(:stream_name, String.Chars.to_string(stream_name))
            |> then(&struct(__MODULE__, &1))

          server_name =
            case decider.registry do
              :disabled -> nil
              :global -> {:global, decider.stream_name}
              {:global, prefix} -> {:global, prefix <> decider.stream_name}
              module -> {:via, Registry, {module, decider.stream_name}}
            end

          %{decider | server_name: server_name}

        {:error, validation_error} ->
          raise ArgumentError, message: Exception.message(validation_error)
      end
    end

    @spec start_server(t()) :: GenServer.on_start() | DynamicSupervisor.on_start_child()
    def start_server(%__MODULE__{} = decider) do
      case decider.supervisor do
        :disabled -> start_link(decider)
        supervisor -> start_supervised(decider, supervisor)
      end
    end

    @spec start_link(t()) :: GenServer.on_start()
    def start_link(%__MODULE__{} = decider) do
      GenServer.start_link(__MODULE__, decider, name: decider.server_name)
    end

    @spec start_supervised(t(), sup :: GenServer.server()) :: DynamicSupervisor.on_start_child()
    def start_supervised(%__MODULE__{} = decider, supervisor) do
      DynamicSupervisor.start_child(supervisor, {__MODULE__, decider})
    end

    @spec query(t() | pid(), Query.t()) :: any()
    def query(decider_or_pid, query) do
      ensure_process_alive!(decider_or_pid, fn pid ->
        GenServer.call(pid, {:query, query})
      end)
    end

    @spec transact(t(), Decision.t(), Codec.ctx()) :: {:ok, t()} | {:error, term()}
    @spec transact(pid(), Decision.t(), Codec.ctx()) :: {:ok, pid()} | {:error, term()}
    def transact(decider_or_pid, decision, ctx \\ nil) do
      ensure_process_alive!(decider_or_pid, fn pid ->
        case GenServer.call(pid, {:transact, decision, ctx}) do
          :ok -> {:ok, decider_or_pid}
          {:error, error} -> {:error, error}
        end
      end)
    end

    defp ensure_process_alive!(pid, fun) when is_pid(pid) do
      if not Process.alive?(pid) do
        raise RuntimeError, message: "Decider: Given process #{inspect(pid)} is not alive"
      else
        fun.(pid)
      end
    end

    defp ensure_process_alive!(%__MODULE__{} = decider, fun) do
      case decider.server_name do
        nil ->
          raise RuntimeError, message: "Decider: On-demand deciders require name (and registry)"

        server_name ->
          try do
            fun.(server_name)
          catch
            :exit, {:noproc, _} -> with({:ok, pid} <- start_server(decider), do: fun.(pid))
          end
      end
    end

    @impl GenServer
    def init(%__MODULE__{} = decider) do
      (decider.on_init || fn -> nil end).()

      stateless_decider =
        Stateless.for_stream(
          decider.stream_name,
          store: decider.store,
          codec: decider.codec,
          fold: decider.fold,
          max_load_attempts: decider.max_load_attempts,
          max_sync_attempts: decider.max_sync_attempts,
          max_resync_attempts: decider.max_resync_attempts
        )

      server = %{decider: stateless_decider, lifetime: decider.lifetime}
      {:ok, server, {:continue, :load}}
    end

    @impl GenServer
    def handle_continue(:load, server) do
      loaded_decider = Stateless.load(server.decider)

      {:noreply, %{server | decider: loaded_decider},
       server.lifetime.after_init(loaded_decider.state.value)}
    end

    @impl GenServer
    def handle_call({:query, query}, _from, server) do
      {:reply, Stateless.query(server.decider, query), server,
       server.lifetime.after_query(server.decider.state.value)}
    end

    @impl GenServer
    def handle_call({:transact, decision, ctx}, _from, server) do
      case Stateless.transact(server.decider, decision, ctx) do
        {:ok, updated_decider} ->
          {:reply, :ok, %{server | decider: updated_decider},
           server.lifetime.after_transact(updated_decider.state.value)}

        {:error, error} ->
          {:reply, {:error, error}, server,
           server.lifetime.after_transact(server.decider.state.value)}
      end
    end

    @impl GenServer
    def handle_info(:timeout, server) do
      {:stop, :normal, server}
    end
  end

  @spec load(Stateful.t()) :: Stateful.t()
  @spec load(Stateless.t()) :: Stateless.t()
  def load(decider) do
    case decider do
      %Stateful{} = decider ->
        case Stateful.start_server(decider) do
          {:ok, pid} ->
            if(decider.server_name, do: decider, else: pid)

          {:error, error_term} ->
            raise RuntimeError, message: "Failed to start Decider process: #{inspect(error_term)}"
        end

      %Stateless{} = decider ->
        Stateless.load(decider)
    end
  end

  @spec query(pid(), Query.t()) :: any()
  @spec query(Stateful.t(), Query.t()) :: any()
  @spec query(Stateless.t(), Query.t()) :: any()
  def query(decider, query) do
    case decider do
      pid when is_pid(pid) -> Stateful.query(pid, query)
      %Stateful{} = decider -> Stateful.query(decider, query)
      %Stateless{} = decider -> Stateless.query(decider, query)
    end
  end

  @spec transact(pid(), Decision.t(), Codec.ctx()) ::
          {:ok, pid()} | {:error, term()}
  @spec transact(Stateful.t(), Decision.t(), Codec.ctx()) ::
          {:ok, Stateful.t()} | {:error, term()}
  @spec transact(Stateless.t(), Decision.t(), Codec.ctx()) ::
          {:ok, Stateless.t()} | {:error, term()}
  def transact(decider, decision, ctx \\ nil) do
    case decider do
      pid when is_pid(pid) -> Stateful.transact(pid, decision, ctx)
      %Stateful{} = decider -> Stateful.transact(decider, decision, ctx)
      %Stateless{} = decider -> Stateless.transact(decider, decision, ctx)
    end
  end
end
