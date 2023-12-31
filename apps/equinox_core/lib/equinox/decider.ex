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

  defmodule ExhaustedLoadAttempts do
    defexception [:message, :stream_name, :attempts, :exception]

    @type t :: %__MODULE__{
            message: String.t(),
            stream_name: String.t(),
            attempts: pos_integer(),
            exception: Exception.t()
          }

    def exception(opts) do
      stream_name = Keyword.fetch!(opts, :stream_name)
      attempts = Keyword.fetch!(opts, :attempts)
      exception = Keyword.fetch!(opts, :exception)

      message =
        "Load from #{inspect(stream_name)} failed after #{attempts} attempt(s): #{Exception.message(exception)}"

      %__MODULE__{
        message: message,
        stream_name: stream_name,
        exception: exception,
        attempts: attempts
      }
    end
  end

  defmodule ExhaustedSyncAttempts do
    defexception [:message, :stream_name, :attempts, :exception]

    @type t :: %__MODULE__{
            message: String.t(),
            stream_name: String.t(),
            attempts: pos_integer(),
            exception: Exception.t()
          }

    def exception(opts) do
      stream_name = Keyword.fetch!(opts, :stream_name)
      attempts = Keyword.fetch!(opts, :attempts)
      exception = Keyword.fetch!(opts, :exception)

      message =
        "Sync to #{stream_name} failed after #{attempts} attempt(s): #{Exception.message(exception)}"

      %__MODULE__{
        message: message,
        stream_name: stream_name,
        exception: exception,
        attempts: attempts
      }
    end
  end

  defmodule ExhaustedResyncAttempts do
    defexception [:message, :stream_name, :attempts, :exception]

    @type t :: %__MODULE__{
            message: String.t(),
            stream_name: String.t(),
            attempts: non_neg_integer(),
            exception: Exception.t()
          }

    def exception(opts) do
      stream_name = Keyword.fetch!(opts, :stream_name)
      attempts = Keyword.fetch!(opts, :attempts)
      exception = Keyword.fetch!(opts, :exception)

      message =
        "Failed to resync with #{stream_name} after #{attempts} attempt(s): #{Exception.message(exception)}"

      %__MODULE__{
        message: message,
        stream_name: stream_name,
        exception: exception,
        attempts: attempts
      }
    end
  end

  defmodule Stateless do
    alias Equinox.{Telemetry, State, Store, Codec, Fold}
    alias Equinox.Decider.{Query, Decision}

    @enforce_keys [:stream_name, :store, :codec, :fold]
    defstruct state: :not_loaded,
              stream_name: nil,
              store: nil,
              codec: nil,
              fold: nil,
              max_load_attempts: 2,
              max_sync_attempts: 2,
              max_resync_attempts: 1

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
              doc: "How many times (in total) should we try to load the state on load errors"
            ],
            max_sync_attempts: [
              type: :pos_integer,
              doc: "How many times (in total) should we try to sync the state on write errors"
            ],
            max_resync_attempts: [
              type: :non_neg_integer,
              doc: "How many times should we try to resync the state on version conflict"
            ]
          )

    @type option :: unquote(NimbleOptions.option_typespec(@opts))
    @spec for_stream(String.t(), [option()]) :: t()
    def for_stream(stream_name, opts) do
      case NimbleOptions.validate(opts, @opts) do
        {:ok, opts} -> struct(__MODULE__, [{:stream_name, stream_name} | opts])
        {:error, error} -> raise ArgumentError, message: Exception.message(error)
      end
    end

    @spec loaded?(t()) :: boolean()
    def loaded?(%__MODULE__{state: %State{}}), do: true
    def loaded?(%__MODULE__{state: _}), do: false

    @spec load(t()) :: t()
    def load(%__MODULE__{} = decider) do
      load_state_with_retry(decider)
    end

    @spec query(t(), Query.t()) :: any()
    def query(%__MODULE__{} = decider, query_fun) do
      ensure_state_loaded(decider, fn decider ->
        Telemetry.span_decider_query(decider, query_fun, fn ->
          Query.execute(query_fun, decider.state)
        end)
      end)
    end

    @spec transact(t(), Decision.t(), Codec.ctx()) :: {:ok, t()} | {:error, term()}
    def transact(%__MODULE__{} = decider, decision_fun, ctx \\ nil) do
      ensure_state_loaded(decider, fn decider ->
        Telemetry.span_decider_transact(decider, decision_fun, fn ->
          transact_with_resync(decider, decision_fun, ctx)
        end)
      end)
    end

    defp ensure_state_loaded(%__MODULE__{} = decider, fun) do
      loaded_decider = if(not loaded?(decider), do: load(decider), else: decider)
      fun.(loaded_decider)
    end

    defp transact_with_resync(%__MODULE__{} = decider, decision_fun, ctx, resync_attempt \\ 0) do
      decision_result =
        Telemetry.span_decider_decision(decider, decision_fun, fn ->
          Decision.execute(decision_fun, decider.state)
        end)

      case decision_result do
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
                |> Telemetry.span_decider_resync(resync_attempt, fn ->
                  load_state_with_retry(decider)
                end)
                |> transact_with_resync(decision_fun, ctx, resync_attempt + 1)
              else
                reraise ExhaustedResyncAttempts,
                        [
                          stream_name: decider.stream_name,
                          exception: version_conflict,
                          attempts: resync_attempt
                        ],
                        __STACKTRACE__
              end
          end
      end
    end

    defp sync_state_with_retry(%__MODULE__{} = decider, ctx, events, sync_attempt \\ 1) do
      try do
        Telemetry.span_decider_sync(decider, events, sync_attempt, fn ->
          synced_state =
            decider.stream_name
            |> decider.store.sync!(decider.state, events, ctx, decider.codec, decider.fold)

          %{decider | state: synced_state}
        end)
      rescue
        unrecoverable in [Store.StreamVersionConflict, Codec.CodecError, Fold.FoldError] ->
          reraise unrecoverable, __STACKTRACE__

        recoverable ->
          if sync_attempt < decider.max_sync_attempts do
            sync_state_with_retry(decider, ctx, events, sync_attempt + 1)
          else
            reraise ExhaustedSyncAttempts,
                    [
                      stream_name: decider.stream_name,
                      exception: recoverable,
                      attempts: sync_attempt
                    ],
                    __STACKTRACE__
          end
      end
    end

    defp load_state_with_retry(%__MODULE__{} = decider, load_attempt \\ 1) do
      try do
        Telemetry.span_decider_load(decider, load_attempt, fn ->
          current_state =
            if(loaded?(decider), do: decider.state, else: State.init(decider.fold))

          loaded_state =
            decider.stream_name
            |> decider.store.load!(current_state, decider.codec, decider.fold)

          %{decider | state: loaded_state}
        end)
      rescue
        unrecoverable in [Codec.CodecError, Fold.FoldError] ->
          reraise unrecoverable, __STACKTRACE__

        recoverable ->
          if load_attempt < decider.max_load_attempts do
            load_state_with_retry(decider, load_attempt + 1)
          else
            reraise ExhaustedLoadAttempts,
                    [
                      stream_name: decider.stream_name,
                      exception: recoverable,
                      attempts: load_attempt
                    ],
                    __STACKTRACE__
          end
      end
    end
  end

  defmodule Stateful do
    use GenServer, restart: :transient

    alias Equinox.Decider.Stateless
    alias Equinox.Codec

    @enforce_keys [:supervisor, :registry, :lifetime]
    defstruct [:server_name, :stateless, :supervisor, :registry, :lifetime, :on_init]

    @type t :: %__MODULE__{}

    @opts NimbleOptions.new!(
            supervisor: [
              type: {:or, [:atom, {:in, [:disabled]}]},
              required: true,
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
              required: true,
              doc: "Name of the Registry (or :global) under which our decider should be listed"
            ],
            lifetime: [
              type: :atom,
              required: true,
              doc: "Process lifetime defition module that implements `Equinox.Lifetime` behaviour"
            ],
            on_init: [
              type: {:fun, 0},
              doc: "Function to execute inside spawned process just before it is initialized"
            ]
          )

    @type option :: unquote(NimbleOptions.option_typespec(@opts))
    @spec wrap_stateless(Stateless.t(), [option()]) :: t()
    def wrap_stateless(%Stateless{} = stateless, opts) do
      case NimbleOptions.validate(opts, @opts) do
        {:ok, opts} ->
          settings = struct(__MODULE__, [{:stateless, stateless} | opts])

          server_name =
            case settings.registry do
              :disabled -> nil
              :global -> {:global, stateless.stream_name}
              {:global, prefix} -> {:global, prefix <> stateless.stream_name}
              module -> {:via, Registry, {module, stateless.stream_name}}
            end

          %{settings | server_name: server_name}

        {:error, validation_error} ->
          raise ArgumentError, message: Exception.message(validation_error)
      end
    end

    @spec start(t()) :: t() | pid()
    def start(%__MODULE__{} = settings) do
      with {:ok, pid} <- start_server(settings) do
        if(settings.server_name, do: settings, else: pid)
      else
        {:error, {:already_started, pid}} -> pid
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

    @spec transact(t(), Decision.t(), Codec.ctx()) :: {:ok, t()} | {:error, term()}
    @spec transact(pid(), Decision.t(), Codec.ctx()) :: {:ok, pid()} | {:error, term()}
    def transact(settings_or_pid, decision, ctx \\ nil) do
      ensure_server_started(settings_or_pid, fn server_name_or_pid ->
        case GenServer.call(server_name_or_pid, {:transact, decision, ctx}) do
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
      (settings.on_init || fn -> nil end).()
      {:ok, %{decider: settings.stateless, lifetime: settings.lifetime}, {:continue, :load}}
    end

    @impl GenServer
    def handle_continue(:load, %{decider: decider} = server) do
      loaded_decider =
        if(not Stateless.loaded?(decider), do: Stateless.load(decider), else: decider)

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

  @spec stateless(String.t(), [Stateless.option()]) :: Stateless.t()
  def stateless(stream_name, opts) when is_bitstring(stream_name) do
    Stateless.for_stream(stream_name, opts)
  end

  @spec stateful(String.t(), [Stateless.option() | Stateful.option()]) :: Stateful.t()
  def stateful(stream_name, opts) when is_bitstring(stream_name) do
    {stateful_opts, stateless_opts} =
      Keyword.split(opts, [:supervisor, :registry, :lifetime, :on_init])

    stream_name
    |> stateless(stateless_opts)
    |> stateful(stateful_opts)
  end

  @spec stateful(Stateless.t(), [Stateful.option()]) :: Stateful.t()
  def stateful(%Stateless{} = stateless, opts), do: Stateful.wrap_stateless(stateless, opts)

  @spec load(Stateless.t()) :: Stateless.t()
  def load(%Stateless{} = stateless), do: Stateless.load(stateless)

  @spec load(String.t(), [Stateless.option()]) :: Stateless.t()
  def load(stream_name, opts) when is_bitstring(stream_name) do
    stream_name
    |> stateless(opts)
    |> load()
  end

  @spec start(Stateful.t()) :: Stateful.t() | pid()
  def start(%Stateful{} = stateful), do: Stateful.start(stateful)

  @spec start(String.t(), [Stateless.option() | Stateful.option()]) :: Stateful.t() | pid()
  @spec start(Stateless.t(), [Stateful.option()]) :: Stateful.t() | pid()
  def start(stream_name_or_stateless, stateful_or_both_opts) do
    stream_name_or_stateless
    |> stateful(stateful_or_both_opts)
    |> start()
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
