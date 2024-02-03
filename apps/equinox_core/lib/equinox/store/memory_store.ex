defmodule Equinox.Store.MemoryStore do
  alias Equinox.Codec.StreamName
  alias Equinox.Events.TimelineEvent
  alias Equinox.Decider.LoadPolicy
  alias Equinox.Store.State

  defmodule NotCheckedOut do
    defexception [:message]
  end

  @enforce_keys [:owner_pid, :codec, :fold]
  defstruct [:owner_pid, :codec, :fold]

  def new(opts \\ []), do: struct!(__MODULE__, Keyword.put_new(opts, :owner_pid, self()))

  defimpl Equinox.Store do
    alias Equinox.Store.MemoryStore

    @impl Equinox.Store
    def load(store, stream, policy) do
      MemoryStore.load(store, stream, policy)
    end

    @impl Equinox.Store
    def sync(store, stream, origin_state, events_to_sync) do
      MemoryStore.sync(store, stream, origin_state, events_to_sync)
    end
  end

  @this {:global, __MODULE__}

  def start_link(_opts) do
    case GenServer.start_link(__MODULE__, :ok, name: @this) do
      {:error, {:already_started, _}} -> :ignore
      other -> other
    end
  end

  def checkout(owner_pid \\ self()) do
    GenServer.call(@this, {:checkout, owner_pid})
  end

  def add_listener(owner_pid \\ self(), listener_pid) do
    GenServer.call(@this, {:add_listener, owner_pid, listener_pid})
  end

  def load(%__MODULE__{} = store, stream, policy) do
    GenServer.call(@this, {:load, store, stream, policy})
  end

  def sync(%__MODULE__{} = store, stream, state, events) do
    GenServer.call(@this, {:sync, store, stream, state, events})
  end

  def inspect(owner_pid \\ self(), stream_name_or_prefix) do
    case stream_name_or_prefix do
      %StreamName{whole: whole} -> GenServer.call(@this, {:inspect, owner_pid, whole})
      prefix when is_bitstring(prefix) -> GenServer.call(@this, {:inspect, owner_pid, prefix})
    end
  end

  use GenServer

  @impl GenServer
  def init(:ok) do
    {:ok, %{stores: %{}, listeners: %{}}}
  end

  @impl GenServer
  def handle_call({:checkout, owner_pid}, _from, state) do
    if not Map.has_key?(state.stores, owner_pid) do
      Process.monitor(owner_pid)
      {:reply, :ok, put_in(state.stores[owner_pid], %{})}
    else
      {:reply, {:error, {:already_checked_out, owner_pid}}, state}
    end
  end

  @impl GenServer
  def handle_call({:load, store, stream_name, policy}, _from, state) do
    cond do
      not Map.has_key?(state.stores, store.owner_pid) ->
        {:reply, {:error, NotCheckedOut.exception("Store has not been checked out")}, state}

      policy.assumes_empty? ->
        {:reply, {:ok, State.new(store.fold.initial(), -1)}, state}

      :otherwise ->
        stream_state =
          state
          |> get_in([:stores, store.owner_pid, stream_name, :events])
          |> Kernel.||([])
          |> fold_stream_events(State.new(store.fold.initial(), -1), store.codec, store.fold)

        {:reply, {:ok, stream_state}, state}
    end
  end

  @impl GenServer
  def handle_call({:sync, store, stream_name, origin_state, events_to_sync}, _from, state) do
    cond do
      not Map.has_key?(state.stores, store.owner_pid) ->
        {:reply, {:error, NotCheckedOut.exception("Store has not been checked out")}, state}

      (get_in(state, [:stores, store.owner_pid, stream_name, :version]) || -1) !=
          origin_state.version ->
        {:reply, {:conflict, fn -> load(store, stream_name, LoadPolicy.new(:default)) end}, state}

      :otherwise ->
        new_events = encode_events(events_to_sync, origin_state, stream_name, store.codec)
        new_state = fold_domain_events(events_to_sync.events, origin_state, store.fold)

        Enum.each(state.listeners[store.owner_pid] || [], fn pid ->
          Enum.each(new_events, &send(pid, &1))
        end)

        {:reply, {:ok, new_state},
         update_in(state, [:stores, store.owner_pid, stream_name], fn
           nil -> %{events: new_events, version: new_state.version}
           %{events: stream} -> %{events: stream ++ new_events, version: new_state.version}
         end)}
    end
  end

  @impl GenServer
  def handle_call({:add_listener, owner_pid, listener_pid}, _from, state) do
    {:reply, :ok,
     update_in(state.listeners[owner_pid], fn
       nil -> MapSet.new([listener_pid])
       set -> MapSet.put(set, listener_pid)
     end)}
  end

  @impl GenServer
  def handle_call({:inspect, owner_pid, prefix}, _from, state) do
    if Map.has_key?(state.stores, owner_pid) do
      {:reply,
       {:ok,
        state.stores[owner_pid]
        |> Enum.filter(fn {name, _stream} -> String.starts_with?(name.whole, prefix) end)
        |> Enum.into(%{}, fn {name, stream} -> {name, stream.events} end)}, state}
    else
      {:reply, {:error, NotCheckedOut.exception("Store has not been checked out")}, state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _, _, pid, _}, state) do
    {_, state} = pop_in(state.stores[pid])
    {_, state} = pop_in(state.listeners[pid])
    {:noreply, state}
  end

  defp encode_events(events_to_sync, origin_state, stream_name, codec) do
    events_to_sync.events
    |> Enum.map(&codec.encode(&1, events_to_sync.context))
    |> Enum.with_index(origin_state.version + 1)
    |> Enum.map(fn {evt, pos} ->
      TimelineEvent.new(
        id: evt.id,
        type: evt.type,
        stream_name: stream_name.whole,
        position: pos,
        global_position: System.monotonic_time() + pos,
        data: evt.data,
        metadata: evt.metadata,
        time: NaiveDateTime.utc_now()
      )
    end)
  end

  defp fold_domain_events(events, state, fold) do
    events
    |> fold.fold(state.value)
    |> State.new(state.version + length(events))
  end

  defp fold_stream_events(stream, state, codec, fold) do
    stream
    |> Enum.map(&{codec.decode(&1), &1.position})
    |> Enum.reject(fn {evt, _pos} -> is_nil(evt) end)
    |> Enum.reduce(state, fn {evt, pos}, state ->
      [evt]
      |> fold.fold(state.value)
      |> State.new(pos)
    end)
  end
end
