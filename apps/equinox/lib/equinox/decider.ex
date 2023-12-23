defmodule Equinox.Decider do
  defmodule DeciderError do
    @enforce_keys [:message]
    defexception [:message]
    @type t :: %__MODULE__{message: String.t()}
  end

  defmodule QueryFunction do
    alias Equinox.Fold

    @type t :: (Fold.state() -> any())

    defmodule QueryError do
      @enforce_keys [:message, :exception]
      defexception [:message, :exception]
      @type t :: %__MODULE__{message: String.t(), exception: Exception.t()}
    end

    @spec execute(t(), Fold.state()) :: any()
    def execute(query_fun, stream_state) do
      try do
        query_fun.(stream_state)
      rescue
        exception ->
          reraise QueryError,
                  [message: inspect(exception), exception: exception],
                  __STACKTRACE__
      end
    end
  end

  defmodule DecideFunction do
    alias Equinox.Fold
    alias Equinox.Events.DomainEvent

    @type t :: (Fold.state() -> result())
    @type result ::
            nil
            | DomainEvent.t()
            | list(DomainEvent.t())
            | {:ok, list(DomainEvent.t())}
            | {:error, term()}
    @type normalized_result :: {:ok, list(DomainEvent.t())} | {:error, term()}

    defmodule DecisionError do
      @enforce_keys [:message, :exception]
      defexception [:message, :exception]
      @type t :: %__MODULE__{message: String.t(), exception: Exception.t()}
    end

    @spec execute(t(), Fold.state()) :: normalized_result()
    def execute(decide_fun, stream_state) do
      try do
        case decide_fun.(stream_state) do
          {:ok, events} -> {:ok, List.wrap(events)}
          {:error, error} -> {:error, error}
          event_or_events -> {:ok, List.wrap(event_or_events)}
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
    alias Equinox.{Store, Codec, Fold}
    alias Equinox.Decider.{QueryFunction, DecideFunction}

    @enforce_keys [:stream_name, :store, :codec, :fold]
    defstruct stream_name: nil,
              stream_state: nil,
              stream_position: -1,
              store: nil,
              codec: nil,
              fold: nil,
              opts: []

    @type t :: %__MODULE__{
            stream_name: StreamName.t(),
            store: Store.t(),
            codec: Codec.t(),
            fold: Fold.t(),
            opts: list(option())
          }

    @default_max_load_attempts 3
    @default_max_write_attempts 3
    @default_max_resync_attempts 1

    @type option ::
            {:max_load_attempts, pos_integer()}
            | {:max_write_attempts, pos_integer()}
            | {:max_resync_attempts, pos_integer()}

    @spec for_stream(StreamName.t(), Enumerable.t()) :: t()
    def for_stream(%StreamName{} = stream_name, opts) do
      opts = Keyword.put(opts, :stream_name, stream_name)
      decider = struct!(__MODULE__, opts)
      %{decider | stream_state: decider.fold.initial()}
    end

    @spec load(t()) :: t()
    def load(%__MODULE__{} = decider) do
      load_stream_state_with_retry(decider)
    end

    @spec query(t(), QueryFunction.t()) :: any()
    def query(%__MODULE__{} = decider, query_fun) do
      QueryFunction.execute(query_fun, decider.stream_state)
    end

    @spec transact(t(), DecideFunction.t(), Codec.context()) :: {:ok, t()} | {:error, term()}
    def transact(%__MODULE__{} = decider, decide, context \\ nil) do
      transact_with_resync(decider, decide, context)
    end

    defp transact_with_resync(%__MODULE__{} = decider, decide, context, resync_attempt \\ 0) do
      case DecideFunction.execute(decide, decider.stream_state) do
        {:error, error} ->
          {:error, error}

        {:ok, []} ->
          {:ok, decider}

        {:ok, events} ->
          case write_stream_state_with_retry(decider, events, context) do
            {:error, %Store.StreamVersionConflict{} = exception} ->
              if(resync_attempt >= max_resync_attempts(decider), do: raise(exception))

              decider
              |> load_stream_state_with_retry(1)
              |> transact_with_resync(decide, context, resync_attempt + 1)

            {:error, exception} ->
              raise exception

            {:ok, written_position} ->
              {new_state, new_version} =
                events
                |> Enum.zip((decider.stream_position + 1)..written_position)
                |> then(&Fold.fold_versioned(decider.fold, decider.stream_state, &1))

              {:ok, %{decider | stream_state: new_state, stream_position: new_version}}
          end
      end
    end

    defp write_stream_state_with_retry(%__MODULE__{} = decider, events, context, attempt \\ 1) do
      try do
        events
        |> then(&Codec.encode_domain_events!(decider.codec, context, &1))
        |> then(&decider.store.write_events(decider.stream_name, &1, decider.stream_position))
      rescue
        exception in [Codec.CodecError, Fold.FoldError] ->
          reraise exception, __STACKTRACE__

        exception ->
          if(attempt >= max_write_attempts(decider), do: reraise(exception, __STACKTRACE__))
          write_stream_state_with_retry(decider, events, context, attempt + 1)
      end
    end

    defp load_stream_state_with_retry(%__MODULE__{} = decider, attempt \\ 1) do
      try do
        {new_state, new_version} =
          decider.stream_name
          |> decider.store.fetch_events(decider.stream_position)
          |> then(&Codec.decode_timeline_events_with_indexes!(decider.codec, &1))
          |> then(&Fold.fold_versioned(decider.fold, decider.stream_state, &1))

        %{decider | stream_state: new_state, stream_position: new_version}
      rescue
        exception in [Codec.CodecError, Fold.FoldError] ->
          reraise exception, __STACKTRACE__

        exception ->
          if(attempt >= max_load_attempts(decider), do: reraise(exception, __STACKTRACE__))
          load_stream_state_with_retry(decider, attempt + 1)
      end
    end

    defp max_load_attempts(%__MODULE__{} = decider),
      do: Keyword.get(decider.opts, :max_load_attempts, @default_max_load_attempts)

    defp max_write_attempts(%__MODULE__{} = decider),
      do: Keyword.get(decider.opts, :max_write_attempts, @default_max_write_attempts)

    defp max_resync_attempts(%__MODULE__{} = decider),
      do: Keyword.get(decider.opts, :max_resync_attempts, @default_max_resync_attempts)
  end

  defmodule Stateful do
    use GenServer, restart: :transient

    alias Equinox.Stream.StreamName
    alias Equinox.Decider.Stateless
    alias Equinox.{Decider, Lifetime, Store, Codec, Fold}

    @enforce_keys [:stream_name, :supervisor, :registry, :store, :codec, :fold]
    defstruct stream_name: nil,
              process_name: nil,
              supervisor: nil,
              registry: nil,
              lifetime: Lifetime.Default,
              store: nil,
              codec: nil,
              fold: nil,
              opts: []

    @type option ::
            Stateless.option()
            | {:on_init, (-> nil)}

    @type t :: %__MODULE__{
            stream_name: StreamName.t(),
            process_name: GenServer.server(),
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
      decider = struct!(__MODULE__, Keyword.put(opts, :stream_name, stream_name))

      process_name =
        case decider.registry do
          :disabled -> nil
          :global -> {:global, String.Chars.to_string(decider.stream_name)}
          module -> {:via, Registry, {module, String.Chars.to_string(decider.stream_name)}}
        end

      %{decider | process_name: process_name}
    end

    @spec to_stateless(t()) :: Stateless.t()
    def to_stateless(%__MODULE__{} = decider) do
      stream_name = decider.stream_name
      opts = decider |> Map.take([:store, :codec, :fold, :opts]) |> Map.to_list()
      Stateless.for_stream(stream_name, opts)
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
      GenServer.start_link(__MODULE__, decider, name: decider.process_name)
    end

    @spec start_supervised(t(), module()) :: DynamicSupervisor.on_start_child()
    def start_supervised(%__MODULE__{} = decider, supervisor) do
      DynamicSupervisor.start_child(supervisor, {__MODULE__, decider})
    end

    @spec query(t() | pid(), QueryFunction.t()) :: any()
    def query(decider_or_pid, query) do
      ensure_process_alive!(decider_or_pid, fn pid ->
        GenServer.call(pid, {:query, query})
      end)
    end

    @spec transact(t(), DecideFunction.t(), Codec.context()) ::
            {:ok, t()} | {:error, term()}
    @spec transact(pid(), DecideFunction.t(), Codec.context()) ::
            {:ok, pid()} | {:error, term()}
    def transact(decider_or_pid, decide, context \\ nil) do
      ensure_process_alive!(decider_or_pid, fn pid ->
        case GenServer.call(pid, {:transact, decide, context}) do
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
      case decider.process_name do
        nil ->
          raise Decider.DeciderError,
            message: "ensure_process_alive!: On-demand spawning requires registry"

        via_registry ->
          try do
            fun.(via_registry)
          catch
            :exit, {:noproc, _} -> with({:ok, pid} <- start_server(decider), do: fun.(pid))
          end
      end
    end

    # Server (callbacks)

    @impl GenServer
    def init(%__MODULE__{} = decider) do
      Keyword.get(decider.opts, :on_init, fn -> nil end).()
      state = %{decider: to_stateless(decider), lifetime: decider.lifetime}
      {:ok, state, {:continue, :load}}
    end

    @impl GenServer
    def handle_continue(:load, state) do
      decider = Stateless.load(state.decider)
      {:noreply, %{state | decider: decider}, state.lifetime.after_init(decider.stream_state)}
    end

    @impl GenServer
    def handle_call({:query, query}, _from, state) do
      {:reply, Stateless.query(state.decider, query), state,
       state.lifetime.after_query(state.decider.stream_state)}
    end

    @impl GenServer
    def handle_call({:transact, decide, context}, _from, state) do
      case Stateless.transact(state.decider, decide, context) do
        {:ok, decider} ->
          {:reply, :ok, %{state | decider: decider},
           state.lifetime.after_transact(decider.stream_state)}

        {:error, error} ->
          {:reply, {:error, error}, state,
           state.lifetime.after_transact(state.decider.stream_state)}
      end
    end

    @impl GenServer
    def handle_info(:timeout, state) do
      {:stop, :normal, state}
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

  @spec transact(pid(), DecideFunction.t(), Codec.context()) ::
          {:ok, pid()} | {:error, term()}
  @spec transact(Stateful.t(), DecideFunction.t(), Codec.context()) ::
          {:ok, Stateful.t()} | {:error, term()}
  @spec transact(Stateless.t(), DecideFunction.t(), Codec.context()) ::
          {:ok, Stateless.t()} | {:error, term()}
  def transact(decider, decide, context \\ nil) do
    case decider do
      pid when is_pid(pid) -> Stateful.transact(pid, decide, context)
      %Stateful{} = decider -> Stateful.transact(decider, decide, context)
      %Stateless{} = decider -> Stateless.transact(decider, decide, context)
    end
  end
end
