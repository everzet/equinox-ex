defmodule Equinox.Decider do
  use GenServer

  alias Equinox.Stream.StreamName
  alias Equinox.Events.DomainEvent
  alias Equinox.{Store, Codec, Fold}

  @enforce_keys [:stream_name, :supervisor, :registry, :store, :codec, :fold]
  defstruct stream_name: nil,
            store: nil,
            codec: nil,
            fold: nil,
            max_reload_attempts: 3,
            max_write_attempts: 3,
            max_resync_attempts: 1,
            supervisor: nil,
            registry: nil,
            on_init: nil

  @type t :: %__MODULE__{
          stream_name: StreamName.t(),
          store: Store.t(),
          codec: Codec.t(),
          fold: Fold.t(),
          max_reload_attempts: pos_integer(),
          max_write_attempts: pos_integer(),
          max_resync_attempts: pos_integer(),
          supervisor: :disabled | GenServer.server(),
          registry: :disabled | :global | GenServer.server(),
          on_init: (-> nil)
        }

  @type query :: (Fold.state() -> any())
  @type decision :: (Fold.state() -> nil | DomainEvent.t() | list(DomainEvent.t()))

  defmodule DeciderError do
    defexception [:message]
    @type t :: %__MODULE__{}
  end

  defmodule Stateless do
    alias Equinox.{Store, Codec, Fold}
    alias Equinox.Events.DomainEvent
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

    @type query_function :: (Fold.state() -> any())
    @type decision_function ::
            (Fold.state() ->
               nil
               | DomainEvent.t()
               | list(DomainEvent.t())
               | {:ok, list(DomainEvent.t())}
               | {:error, term()})
    @type context :: any()

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

    @spec query(t(), query_function()) :: any()
    def query(%__MODULE__{} = decider, query_fun) do
      query_fun.(decider.stream_state)
    end

    @spec transact(t(), decision_function(), context()) :: {:ok, t()} | {:error, term()}
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
          |> fetch_timeline_events!(decider.stream_position, decider.store)
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

    defp fetch_timeline_events!(stream_name, from_position, store) do
      store.fetch_events(stream_name, from_position)
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
          {codec.decode!(timeline_event), timeline_event.position}
        rescue
          exception ->
            reraise Codec.CodecError,
                    [message: "#{inspect(codec)}.decode!: #{exception.message}"],
                    __STACKTRACE__
        end
      end)
    end

    defp encode_domain_events_into_event_data!(domain_events, change_context, codec) do
      Enum.map(domain_events, fn domain_event ->
        try do
          codec.encode!(domain_event, change_context)
        rescue
          exception ->
            reraise Codec.CodecError,
                    [message: "#{inspect(codec)}.encode!: #{exception.message}"],
                    __STACKTRACE__
        end
      end)
    end

    defp fold_domain_events!(domain_events, state, position, fold) do
      Enum.reduce(domain_events, {state, position}, fn {event, position}, {state, _} ->
        try do
          {fold.evolve!(state, event), position}
        rescue
          exception ->
            reraise Fold.FoldError,
                    [message: "#{inspect(fold)}.evolve!: #{exception.message}"],
                    __STACKTRACE__
        end
      end)
    end

    defp max_load_attempts(%__MODULE__{} = decider),
      do: Keyword.get(decider.opts, :max_load_attempts, @default_max_load_attempts)

    defp max_write_attempts(%__MODULE__{} = decider),
      do: Keyword.get(decider.opts, :max_write_attempts, @default_max_write_attempts)

    defp max_resync_attempts(%__MODULE__{} = decider),
      do: Keyword.get(decider.opts, :max_resync_attempts, @default_max_resync_attempts)
  end

  # Client API

  @spec start_supervised(t()) :: DynamicSupervisor.on_start_child()
  def start_supervised(%__MODULE__{} = decider) do
    case decider.supervisor do
      nil ->
        {:error,
         %DeciderError{
           message: "start_supervised: Supervision requires supervisor, but it is disabled"
         }}

      sup ->
        DynamicSupervisor.start_child(sup, {__MODULE__, decider})
    end
  end

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = decider) do
    GenServer.start_link(__MODULE__, decider, name: process_name(decider))
  end

  @spec query(t() | pid(), query()) :: {:ok, result :: any()} | {:error, Exception.t()}
  def query(decider_or_pid, query) do
    ensure_process_alive(decider_or_pid, fn pid ->
      GenServer.call(pid, {:query, query})
    end)
  end

  @spec transact(t() | pid(), decision(), context :: any()) :: :ok | {:error, term()}
  def transact(decider_or_pid, decision, context \\ nil) do
    ensure_process_alive(decider_or_pid, fn pid ->
      GenServer.call(pid, {:transact, decision, context})
    end)
  end

  defp ensure_process_alive(decider_or_pid, fun) do
    case decider_or_pid do
      pid when is_pid(pid) ->
        fun.(pid)

      %__MODULE__{} = decider ->
        case process_name(decider) do
          nil ->
            {:error,
             %DeciderError{
               message:
                 "ensure_process_alive: On-demand spawning requires registry, but it is disabled"
             }}

          via_registry ->
            try do
              fun.(via_registry)
            catch
              :exit, {:noproc, _} -> with({:ok, pid} <- start_process(decider), do: fun.(pid))
            end
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
    Map.get(decider, :on_init, fn -> nil end).()

    server_state =
      Stateless.for_stream(
        decider.stream_name,
        store: decider.store,
        codec: decider.codec,
        fold: decider.fold,
        opts: [
          max_load_attempts: decider.max_reload_attempts,
          max_write_attempts: decider.max_write_attempts,
          max_resync_attempts: decider.max_resync_attempts
        ]
      )

    {:ok, server_state, {:continue, :reload_stream_state}}
  end

  @impl GenServer
  def handle_continue(:reload_stream_state, server_state) do
    {:noreply, Stateless.load(server_state)}
  end

  @impl GenServer
  def handle_call({:query, query}, _from, server_state) do
    {:reply, Stateless.query(server_state, query), server_state}
  end

  @impl GenServer
  def handle_call({:transact, decision, context}, _from, server_state) do
    case Stateless.transact(server_state, decision, context) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      {:error, error} -> {:reply, {:error, error}, server_state}
    end
  end
end
