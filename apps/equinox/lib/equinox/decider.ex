defmodule Equinox.Decider do
  defmodule DeciderError do
    @enforce_keys [:message]
    defexception [:message]
    @type t :: %__MODULE__{message: String.t()}
  end

  defmodule QueryFunction do
    alias Equinox.State

    @type t :: (State.value() -> any())

    defmodule QueryError do
      @enforce_keys [:message, :exception]
      defexception [:message, :exception]
      @type t :: %__MODULE__{message: String.t(), exception: Exception.t()}
    end

    @spec execute(t(), State.value()) :: any()
    def execute(query_fun, state_value) do
      try do
        query_fun.(state_value)
      rescue
        exception ->
          reraise QueryError,
                  [message: inspect(exception), exception: exception],
                  __STACKTRACE__
      end
    end
  end

  defmodule DecideFunction do
    alias Equinox.State
    alias Equinox.Events.DomainEvent

    @type t ::
            (State.value() ->
               nil
               | DomainEvent.t()
               | list(DomainEvent.t())
               | {:ok, DomainEvent.t() | list(DomainEvent.t())}
               | {:error, term()})

    defmodule DecisionError do
      @enforce_keys [:message, :exception]
      defexception [:message, :exception]
      @type t :: %__MODULE__{message: String.t(), exception: Exception.t()}
    end

    @spec execute(t(), State.value()) :: {:ok, list(DomainEvent.t())} | {:error, term()}
    def execute(decide_fun, state_value) do
      try do
        case decide_fun.(state_value) do
          {:error, error} -> {:error, error}
          {:ok, event_or_events} -> {:ok, List.wrap(event_or_events)}
          nil_or_event_or_events -> {:ok, List.wrap(nil_or_event_or_events)}
        end
      rescue
        exception ->
          reraise DecisionError,
                  [message: inspect(exception), exception: exception],
                  __STACKTRACE__
      end
    end
  end

  defmodule Stateless do
    alias Equinox.Stream.StreamName
    alias Equinox.{State, Store, Codec, Fold}
    alias Equinox.Decider.{QueryFunction, DecideFunction}

    @enforce_keys [:stream_name, :store, :codec, :fold]
    defstruct stream_name: nil,
              state: nil,
              store: nil,
              codec: nil,
              fold: nil,
              opts: []

    @type t :: %__MODULE__{
            stream_name: String.t(),
            state: State.t(),
            store: Store.t(),
            codec: Codec.t(),
            fold: Fold.t(),
            opts: list(option())
          }

    @type option ::
            {:max_load_attempts, pos_integer()}
            | {:max_sync_attempts, pos_integer()}
            | {:max_resync_attempts, non_neg_integer()}

    @default_opts [
      max_load_attempts: 2,
      max_sync_attempts: 2,
      max_resync_attempts: 1
    ]

    @spec for_stream(StreamName.t(), Enumerable.t()) :: t()
    def for_stream(%StreamName{} = stream_name, opts) do
      decider =
        opts
        |> Keyword.put(:stream_name, String.Chars.to_string(stream_name))
        |> Keyword.update(:opts, @default_opts, &Keyword.merge(@default_opts, &1))
        |> then(&struct!(__MODULE__, &1))

      %{decider | state: State.init(decider.fold)}
    end

    @spec load(t()) :: t()
    def load(%__MODULE__{} = decider) do
      load_state_with_retry(decider)
    end

    @spec query(t(), QueryFunction.t()) :: any()
    def query(%__MODULE__{} = decider, query_fun) do
      QueryFunction.execute(query_fun, decider.state.value)
    end

    @spec transact(t(), DecideFunction.t(), Codec.ctx()) :: {:ok, t()} | {:error, term()}
    def transact(%__MODULE__{} = decider, decide, ctx \\ nil) do
      transact_with_resync(decider, decide, ctx)
    end

    defp transact_with_resync(%__MODULE__{} = decider, decide, ctx, resync_attempt \\ 0) do
      case DecideFunction.execute(decide, decider.state.value) do
        {:error, error} ->
          {:error, error}

        {:ok, []} ->
          {:ok, decider}

        {:ok, events} ->
          try do
            {:ok, sync_state_with_retry(decider, ctx, events)}
          rescue
            exception in [Store.StreamVersionConflict] ->
              if(resync_attempt >= max_resync_attempts(decider), do: raise(exception))

              decider
              |> load_state_with_retry()
              |> transact_with_resync(decide, ctx, resync_attempt + 1)
          end
      end
    end

    defp sync_state_with_retry(%__MODULE__{} = decider, ctx, events, attempt \\ 1) do
      try do
        new_state =
          decider.store.sync!(
            decider.stream_name,
            decider.state,
            events,
            ctx,
            decider.codec,
            decider.fold
          )

        %{decider | state: new_state}
      rescue
        exception in [Store.StreamVersionConflict, Codec.CodecError, Fold.FoldError] ->
          reraise exception, __STACKTRACE__

        exception ->
          if(attempt >= max_sync_attempts(decider), do: reraise(exception, __STACKTRACE__))
          sync_state_with_retry(decider, ctx, events, attempt + 1)
      end
    end

    defp load_state_with_retry(%__MODULE__{} = decider, attempt \\ 1) do
      try do
        new_state =
          decider.store.load!(decider.stream_name, decider.state, decider.codec, decider.fold)

        %{decider | state: new_state}
      rescue
        exception in [Codec.CodecError, Fold.FoldError] ->
          reraise exception, __STACKTRACE__

        exception ->
          if(attempt >= max_load_attempts(decider), do: reraise(exception, __STACKTRACE__))
          load_state_with_retry(decider, attempt + 1)
      end
    end

    defp max_load_attempts(%__MODULE__{opts: o}), do: Keyword.fetch!(o, :max_load_attempts)
    defp max_sync_attempts(%__MODULE__{opts: o}), do: Keyword.fetch!(o, :max_sync_attempts)
    defp max_resync_attempts(%__MODULE__{opts: o}), do: Keyword.fetch!(o, :max_resync_attempts)
  end

  defmodule Stateful do
    use GenServer, restart: :transient

    alias Equinox.Stream.StreamName
    alias Equinox.Decider.Stateless
    alias Equinox.{Decider, Lifetime, Store, Codec, Fold}

    @enforce_keys [:stream_name, :supervisor, :registry, :store, :codec, :fold]
    defstruct stream_name: nil,
              server_name: nil,
              supervisor: nil,
              registry: nil,
              lifetime: Lifetime.StayAliveFor30Seconds,
              store: nil,
              codec: nil,
              fold: nil,
              opts: []

    @type option ::
            Stateless.option()
            | {:on_init, (-> nil)}

    @type t :: %__MODULE__{
            stream_name: StreamName.t(),
            server_name: GenServer.server(),
            supervisor: :disabled | GenServer.server(),
            registry: :disabled | :global | GenServer.server(),
            lifetime: Lifetime.t(),
            store: Store.t(),
            codec: Codec.t(),
            fold: Fold.t(),
            opts: list(option())
          }

    @spec for_stream(StreamName.t(), Enumerable.t()) :: t()
    def for_stream(%StreamName{} = stream_name, opts) do
      decider =
        opts
        |> Keyword.put(:stream_name, stream_name)
        |> then(&struct!(__MODULE__, &1))

      server_name =
        case decider.registry do
          :disabled -> nil
          :global -> {:global, String.Chars.to_string(decider.stream_name)}
          module -> {:via, Registry, {module, String.Chars.to_string(decider.stream_name)}}
        end

      %{decider | server_name: server_name}
    end

    @spec to_stateless(t()) :: Stateless.t()
    def to_stateless(%__MODULE__{} = decider) do
      Stateless.for_stream(decider.stream_name,
        store: decider.store,
        codec: decider.codec,
        fold: decider.fold,
        opts: decider.opts
      )
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

    @spec query(t() | pid(), QueryFunction.t()) :: any()
    def query(decider_or_pid, query) do
      ensure_process_alive!(decider_or_pid, fn pid ->
        GenServer.call(pid, {:query, query})
      end)
    end

    @spec transact(t(), DecideFunction.t(), Codec.ctx()) :: {:ok, t()} | {:error, term()}
    @spec transact(pid(), DecideFunction.t(), Codec.ctx()) :: {:ok, pid()} | {:error, term()}
    def transact(decider_or_pid, decide, ctx \\ nil) do
      ensure_process_alive!(decider_or_pid, fn pid ->
        case GenServer.call(pid, {:transact, decide, ctx}) do
          :ok -> {:ok, decider_or_pid}
          {:error, error} -> {:error, error}
        end
      end)
    end

    defp ensure_process_alive!(pid, fun) when is_pid(pid) do
      if Process.alive?(pid) do
        fun.(pid)
      else
        raise Decider.DeciderError,
          message: "ensure_process_alive!: Given process #{inspect(pid)} is not alive"
      end
    end

    defp ensure_process_alive!(%__MODULE__{} = decider, fun) do
      case decider.server_name do
        nil ->
          raise Decider.DeciderError,
            message: "ensure_process_alive!: On-demand spawning requires registry"

        server_name ->
          try do
            fun.(server_name)
          catch
            :exit, {:noproc, _} -> with({:ok, pid} <- start_server(decider), do: fun.(pid))
          end
      end
    end

    # Server (callbacks)

    @impl GenServer
    def init(%__MODULE__{} = decider) do
      Keyword.get(decider.opts, :on_init, fn -> nil end).()
      server = %{decider: to_stateless(decider), lifetime: decider.lifetime}
      {:ok, server, {:continue, :load}}
    end

    @impl GenServer
    def handle_continue(:load, server) do
      {:noreply, %{server | decider: Stateless.load(server.decider)},
       server.lifetime.after_init(server.decider.state.value)}
    end

    @impl GenServer
    def handle_call({:query, query}, _from, server) do
      {:reply, Stateless.query(server.decider, query), server,
       server.lifetime.after_query(server.decider.state.value)}
    end

    @impl GenServer
    def handle_call({:transact, decide, ctx}, _from, server) do
      case Stateless.transact(server.decider, decide, ctx) do
        {:ok, decider} ->
          {:reply, :ok, %{server | decider: decider},
           server.lifetime.after_transact(server.decider.state.value)}

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

  @spec query(pid(), QueryFunction.t()) :: any()
  @spec query(Stateful.t(), QueryFunction.t()) :: any()
  @spec query(Stateless.t(), QueryFunction.t()) :: any()
  def query(decider, query) do
    case decider do
      pid when is_pid(pid) -> Stateful.query(pid, query)
      %Stateful{} = decider -> Stateful.query(decider, query)
      %Stateless{} = decider -> Stateless.query(decider, query)
    end
  end

  @spec transact(pid(), DecideFunction.t(), Codec.ctx()) ::
          {:ok, pid()} | {:error, term()}
  @spec transact(Stateful.t(), DecideFunction.t(), Codec.ctx()) ::
          {:ok, Stateful.t()} | {:error, term()}
  @spec transact(Stateless.t(), DecideFunction.t(), Codec.ctx()) ::
          {:ok, Stateless.t()} | {:error, term()}
  def transact(decider, decide, ctx \\ nil) do
    case decider do
      pid when is_pid(pid) -> Stateful.transact(pid, decide, ctx)
      %Stateful{} = decider -> Stateful.transact(decider, decide, ctx)
      %Stateless{} = decider -> Stateless.transact(decider, decide, ctx)
    end
  end
end
