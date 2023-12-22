defmodule Equinox.Decider do
  alias Equinox.Fold
  alias Equinox.Events.DomainEvent

  @type query_function ::
          (Fold.state() -> any())
  @type decision_function ::
          (Fold.state() ->
             nil
             | DomainEvent.t()
             | list(DomainEvent.t())
             | {:ok, list(DomainEvent.t())}
             | {:error, term()})
  @type context :: any()

  defmodule DeciderError do
    defexception [:message]
    @type t :: %__MODULE__{}
  end

  defmodule Stateless do
    alias Equinox.{Decider, Store, Codec, Fold}
    alias Equinox.Stream.StreamName

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
      load_stream_state(decider)
    end

    @spec query(t(), Decider.query_function()) :: any()
    def query(%__MODULE__{} = decider, query_fun) do
      query_fun.(decider.stream_state)
    end

    @spec transact(t(), Decider.decision_function(), Decider.context()) ::
            {:ok, t()} | {:error, term()}
    def transact(%__MODULE__{} = decider, decision_fun, context \\ nil) do
      do_transact(decider, decision_fun, context)
    end

    defp do_transact(%__MODULE__{} = decider, decision_fun, context, resync_attempt \\ 0) do
      decider.stream_state
      |> decision_fun.()
      |> handle_decision_result(decider, fn events ->
        try do
          {:ok, sync_stream_state(decider, events, context)}
        rescue
          exception in [Store.StreamVersionConflict] ->
            if resync_attempt < max_resync_attempts(decider) do
              decider
              |> load_stream_state()
              |> do_transact(decision_fun, context, resync_attempt + 1)
            else
              reraise exception, __STACKTRACE__
            end
        end
      end)
    end

    defp handle_decision_result(decision_result, decider, events_fun) do
      case decision_result do
        {:ok, []} -> {:ok, decider}
        {:ok, events} -> events_fun.(events)
        {:error, error} -> {:error, error}
        noop when noop in [nil, []] -> {:ok, decider}
        event_or_events -> event_or_events |> List.wrap() |> events_fun.()
      end
    end

    defp load_stream_state(%__MODULE__{} = decider, load_attempt \\ 1) do
      try do
        {stream_state, stream_position} =
          decider.stream_name
          |> decider.store.fetch_events(decider.stream_position)
          |> decode_timeline_into_domain_events!(decider.codec)
          |> fold_domain_events!(decider.stream_state, decider.stream_position, decider.fold)

        %{decider | stream_state: stream_state, stream_position: stream_position}
      rescue
        exception in [Codec.CodecError, Fold.FoldError] ->
          reraise exception, __STACKTRACE__

        exception ->
          if load_attempt < max_load_attempts(decider) do
            load_stream_state(decider, load_attempt + 1)
          else
            reraise exception, __STACKTRACE__
          end
      end
    end

    defp sync_stream_state(%__MODULE__{} = decider, domain_events, context, write_attempt \\ 1) do
      try do
        written_position =
          domain_events
          |> encode_domain_events_into_event_data!(context, decider.codec)
          |> write_event_data!(decider.stream_name, decider.stream_position, decider.store)

        {stream_state, stream_position} =
          domain_events
          |> Enum.zip((decider.stream_position + 1)..written_position)
          |> fold_domain_events!(decider.stream_state, decider.stream_position, decider.fold)

        %{decider | stream_state: stream_state, stream_position: stream_position}
      rescue
        exception in [Store.StreamVersionConflict, Codec.CodecError, Fold.FoldError] ->
          reraise exception, __STACKTRACE__

        exception ->
          if write_attempt < max_write_attempts(decider) do
            sync_stream_state(decider, domain_events, context, write_attempt + 1)
          else
            reraise exception, __STACKTRACE__
          end
      end
    end

    defp write_event_data!(events_data, stream_name, expected_version, store) do
      case store.write_events(stream_name, events_data, expected_version) do
        {:ok, written_position} -> written_position
        {:error, error} -> raise error
      end
    end

    defp decode_timeline_into_domain_events!(timeline_events, codec) do
      Stream.map(timeline_events, fn timeline_event ->
        try do
          codec.decode(timeline_event)
        rescue
          exception ->
            reraise Codec.CodecError,
                    [message: "#{inspect(codec)}.decode: #{inspect(exception)}"],
                    __STACKTRACE__
        end
        |> case do
          {:ok, domain_event} -> {domain_event, timeline_event.position}
          {:error, exception} -> raise exception
        end
      end)
    end

    defp encode_domain_events_into_event_data!(domain_events, change_context, codec) do
      Enum.map(domain_events, fn domain_event ->
        try do
          codec.encode(domain_event, change_context)
        rescue
          exception ->
            reraise Codec.CodecError,
                    [message: "#{inspect(codec)}.encode: #{inspect(exception)}"],
                    __STACKTRACE__
        end
        |> case do
          {:ok, timeline_event} -> timeline_event
          {:error, exception} -> raise exception
        end
      end)
    end

    defp fold_domain_events!(domain_events, state, position, fold) do
      Enum.reduce(domain_events, {state, position}, fn {event, position}, {state, _} ->
        try do
          fold.evolve(state, event)
        rescue
          exception ->
            reraise Fold.FoldError,
                    [message: "#{inspect(fold)}.evolve: #{inspect(exception)}"],
                    __STACKTRACE__
        end
        |> then(&{&1, position})
      end)
    end

    defp max_load_attempts(%__MODULE__{} = decider),
      do: Keyword.get(decider.opts, :max_load_attempts, @default_max_load_attempts)

    defp max_write_attempts(%__MODULE__{} = decider),
      do: Keyword.get(decider.opts, :max_write_attempts, @default_max_write_attempts)

    defp max_resync_attempts(%__MODULE__{} = decider),
      do: Keyword.get(decider.opts, :max_resync_attempts, @default_max_resync_attempts)
  end

  defmodule Stateful do
    use GenServer

    alias Equinox.{Decider, Store, Codec, Fold}
    alias Equinox.Stream.StreamName
    alias Equinox.Decider.Stateless

    @enforce_keys [:stream_name, :supervisor, :registry, :store, :codec, :fold]
    defstruct stream_name: nil,
              supervisor: nil,
              registry: nil,
              store: nil,
              codec: nil,
              fold: nil,
              opts: []

    @type option ::
            Stateless.option()
            | {:on_init, (-> nil)}

    @type t :: %__MODULE__{
            stream_name: StreamName.t(),
            supervisor: :disabled | GenServer.server(),
            registry: :disabled | :global | GenServer.server(),
            store: Store.t(),
            codec: Codec.t(),
            fold: Fold.t(),
            opts: list(option())
          }

    @spec for_stream(StreamName.t(), Enumerable.t()) :: t()
    def for_stream(%StreamName{} = stream_name, opts) do
      struct!(__MODULE__, Keyword.put(opts, :stream_name, stream_name))
    end

    @spec to_stateless(t()) :: Stateless.t()
    def to_stateless(%__MODULE__{} = decider) do
      stream_name = decider.stream_name
      opts = decider |> Map.take([:store, :codec, :fold, :opts]) |> Map.to_list()
      Stateless.for_stream(stream_name, opts)
    end

    @spec start_supervised(t()) :: DynamicSupervisor.on_start_child()
    def start_supervised(%__MODULE__{} = decider) do
      case decider.supervisor do
        nil ->
          raise Decider.DeciderError,
            message: "start_supervised: Supervision requires supervisor, but it is disabled"

        sup ->
          DynamicSupervisor.start_child(sup, {__MODULE__, decider})
      end
    end

    @spec start_link(t()) :: GenServer.on_start()
    def start_link(%__MODULE__{} = decider) do
      GenServer.start_link(__MODULE__, decider, name: process_name(decider))
    end

    @spec query(t() | pid(), Decider.query()) :: any()
    def query(decider_or_pid, query) do
      ensure_process_alive!(decider_or_pid, fn pid ->
        GenServer.call(pid, {:query, query})
      end)
    end

    @spec transact(t(), Decider.decision(), Decider.context()) ::
            {:ok, t()} | {:error, term()}
    @spec transact(pid(), Decider.decision(), Decider.context()) ::
            {:ok, pid()} | {:error, term()}
    def transact(decider_or_pid, decision, context \\ nil) do
      ensure_process_alive!(decider_or_pid, fn pid ->
        case GenServer.call(pid, {:transact, decision, context}) do
          :ok -> {:ok, decider_or_pid}
          {:error, error} -> {:error, error}
        end
      end)
    end

    defp ensure_process_alive!(pid, fun) when is_pid(pid) do
      if Process.alive?(pid) do
        fun.(pid)
      else
        raise Decider.DeciderError, message: "ensure_process_alive!: Given process is not alive"
      end
    end

    defp ensure_process_alive!(%__MODULE__{} = decider, fun) do
      case process_name(decider) do
        nil ->
          raise Decider.DeciderError,
            message:
              "ensure_process_alive!: On-demand spawning requires registry, but it is disabled"

        via_registry ->
          try do
            fun.(via_registry)
          catch
            :exit, {:noproc, _} -> with({:ok, pid} <- start_process(decider), do: fun.(pid))
          end
      end
    end

    defp process_name(%__MODULE__{} = decider) do
      case decider.registry do
        :disabled -> nil
        :global -> {:global, String.Chars.to_string(decider.stream_name)}
        module -> {:via, module, String.Chars.to_string(decider.stream_name)}
      end
    end

    defp start_process(%__MODULE__{} = decider) do
      case decider.supervisor do
        :disabled -> start_link(decider)
        _supervisor -> start_supervised(decider)
      end
    end

    # Server (callbacks)

    @impl GenServer
    def init(%__MODULE__{} = decider) do
      on_init = Keyword.get(decider.opts, :on_init, fn -> nil end)
      on_init.()
      {:ok, to_stateless(decider), {:continue, :load}}
    end

    @impl GenServer
    def handle_continue(:load, decider) do
      {:noreply, Stateless.load(decider)}
    end

    @impl GenServer
    def handle_call({:query, query}, _from, decider) do
      {:reply, Stateless.query(decider, query), decider}
    end

    @impl GenServer
    def handle_call({:transact, decision, context}, _from, decider) do
      case Stateless.transact(decider, decision, context) do
        {:ok, new_decider} -> {:reply, :ok, new_decider}
        {:error, error} -> {:reply, {:error, error}, decider}
      end
    end
  end

  @spec query(pid(), query_function()) :: any()
  @spec query(Stateful.t(), query_function()) :: any()
  @spec query(Stateless.t(), query_function()) :: any()
  def query(decider, query) do
    case decider do
      pid when is_pid(pid) -> Stateful.query(pid, query)
      %Stateful{} = decider -> Stateful.query(decider, query)
      %Stateless{} = decider -> Stateless.query(decider, query)
    end
  end

  @spec transact(pid(), decision_function(), context()) ::
          {:ok, pid()} | {:error, term()}
  @spec transact(Stateful.t(), decision_function(), context()) ::
          {:ok, Stateful.t()} | {:error, term()}
  @spec transact(Stateless.t(), decision_function(), context()) ::
          {:ok, Stateless.t()} | {:error, term()}
  def transact(decider, decision, context \\ nil) do
    case decider do
      pid when is_pid(pid) -> Stateful.transact(pid, decision, context)
      %Stateful{} = decider -> Stateful.transact(decider, decision, context)
      %Stateless{} = decider -> Stateless.transact(decider, decision, context)
    end
  end
end
