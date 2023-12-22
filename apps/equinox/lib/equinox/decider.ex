defmodule Equinox.Decider do
  use GenServer

  alias Equinox.Stream.StreamName
  alias Equinox.Events.{TimelineEvent, DomainEvent}
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

  @type decision ::
          (Fold.state() ->
             nil | DomainEvent.t() | list(DomainEvent.t()) | {:error, Exception.t()})
  @type query :: (Fold.state() -> any())

  defmodule DeciderError do
    defexception [:message]
    @type t :: %__MODULE__{}
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
    {:ok, init_server_state(decider), {:continue, :reload_stream_state}}
  end

  @impl GenServer
  def handle_continue(:reload_stream_state, server_state) do
    {:noreply, reload_stream_state(server_state)}
  end

  @impl GenServer
  def handle_call({:query, query}, _from, server_state) do
    {:reply, do_query(query, server_state), server_state}
  end

  @impl GenServer
  def handle_call({:transact, decision, context}, _from, server_state) do
    {reply, new_state} = do_transact(decision, context, server_state)
    {:reply, reply, new_state}
  end

  # Server (internal logic)

  defp init_server_state(%__MODULE__{} = decider) do
    %{
      stream_position: -1,
      stream_state: decider.fold.initial(),
      stream_name: decider.stream_name,
      store: decider.store,
      codec: decider.codec,
      fold: decider.fold,
      max_reload_attempts: decider.max_reload_attempts,
      max_write_attempts: decider.max_write_attempts,
      max_resync_attempts: decider.max_resync_attempts
    }
  end

  defp do_query(query, server_state) do
    try do
      query.(server_state.stream_state)
    rescue
      exception -> {:error, exception}
    end
    |> case do
      {:ok, value} -> {:ok, value}
      {:error, error} -> {:error, error}
      value -> {:ok, value}
    end
  end

  defp do_transact(decision, context, server_state, resync_attempt \\ 0) do
    try do
      decision.(server_state.stream_state)
    rescue
      exception -> {:error, exception}
    end
    |> case do
      {:error, error} ->
        {{:error, error}, server_state}

      noop when noop in [nil, []] ->
        {:ok, server_state}

      result ->
        try do
          new_server_state = result |> List.wrap() |> sync_stream_state(context, server_state)
          {:ok, new_server_state}
        rescue
          exception in [Store.StreamVersionConflict] ->
            if resync_attempt < server_state.max_resync_attempts do
              server_state
              |> reload_stream_state()
              |> then(&do_transact(decision, context, &1, resync_attempt + 1))
            else
              reraise exception, __STACKTRACE__
            end
        end
    end
  end

  defp apply_written_events_to_state(events_data_with_position, server_state) do
    {stream_state, stream_position} =
      events_data_with_position
      |> Stream.map(&TimelineEvent.from_data(server_state.stream_name, elem(&1, 0), elem(&1, 1)))
      |> decode_timeline_events_with_position(server_state.codec)
      |> fold_domain_events_with_position(
        server_state.stream_state,
        server_state.stream_position,
        server_state.fold
      )

    %{server_state | stream_state: stream_state, stream_position: stream_position}
  end

  defp sync_stream_state(domain_events, change_context, server_state, attempt \\ 1) do
    try do
      domain_events
      |> encode_domain_events(change_context, server_state.codec)
      |> write_events(server_state.stream_name, server_state.stream_position, server_state.store)
      |> apply_written_events_to_state(server_state)
    rescue
      exception in [Store.StreamVersionConflict, Codec.CodecError, Fold.FoldError] ->
        reraise exception, __STACKTRACE__

      exception ->
        if attempt < server_state.max_write_attempts do
          sync_stream_state(domain_events, change_context, server_state, attempt + 1)
        else
          reraise exception, __STACKTRACE__
        end
    end
  end

  defp reload_stream_state(server_state, attempt \\ 1) do
    try do
      {stream_state, stream_position} =
        server_state.stream_name
        |> server_state.store.fetch_events(server_state.stream_position)
        |> decode_timeline_events_with_position(server_state.codec)
        |> fold_domain_events_with_position(
          server_state.stream_state,
          server_state.stream_position,
          server_state.fold
        )

      %{server_state | stream_state: stream_state, stream_position: stream_position}
    rescue
      exception in [Codec.CodecError, Fold.FoldError] ->
        reraise exception, __STACKTRACE__

      exception ->
        if attempt < server_state.max_reload_attempts do
          reload_stream_state(server_state, attempt + 1)
        else
          reraise exception, __STACKTRACE__
        end
    end
  end

  defp write_events(events_data, stream_name, expected_version, store) do
    case store.write_events(stream_name, events_data, expected_version) do
      {:ok, written_position} -> Enum.zip(events_data, (expected_version + 1)..written_position)
      {:error, error} -> raise error
    end
  end

  defp decode_timeline_events_with_position(timeline_events, codec) do
    Stream.map(timeline_events, fn timeline_event ->
      try do
        {codec.decode!(timeline_event), timeline_event.position}
      rescue
        exception ->
          reraise Codec.CodecError,
                  [message: "#{inspect(codec)}.decode: #{exception.message}"],
                  __STACKTRACE__
      end
    end)
  end

  defp encode_domain_events(domain_events, change_context, codec) do
    Enum.map(domain_events, fn domain_event ->
      try do
        codec.encode!(domain_event, change_context)
      rescue
        exception ->
          reraise Codec.CodecError,
                  [message: "#{inspect(codec)}.encode: #{exception.message}"],
                  __STACKTRACE__
      end
    end)
  end

  defp fold_domain_events_with_position(domain_events, state, position, fold) do
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
end
