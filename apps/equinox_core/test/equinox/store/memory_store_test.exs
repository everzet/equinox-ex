defmodule Equinox.Store.MemoryStoreTest do
  use ExUnit.Case, async: false

  alias Equinox.{Codec.StreamName, Decider.LoadPolicy}
  alias Equinox.{Store, Store.State, Store.EventsToSync}
  alias Equinox.Events.{EventData, TimelineEvent}
  alias Equinox.Store.MemoryStore

  defmodule NumberCodec do
    @behaviour Equinox.Codec
    def encode(n, %{}), do: EventData.new(type: "Number", data: n)
    def decode(event), do: event.data
  end

  defmodule SumFold do
    @behaviour Equinox.Fold
    def initial(), do: 0
    def fold(nums, sum), do: Enum.sum([sum | nums])
  end

  @stream StreamName.decode!("Invoice-123", 1)
  @state State.new(SumFold.initial(), -1)

  setup do
    start_supervised!({MemoryStore, []})
    :ok
  end

  describe "checkout/1" do
    test "is required for interacting with the store" do
      store = MemoryStore.new(codec: NumberCodec, fold: SumFold)

      assert {:error, %MemoryStore.NotCheckedOut{}} =
               Store.load(store, @stream, LoadPolicy.new(:default))

      assert {:error, %MemoryStore.NotCheckedOut{}} =
               Store.sync(store, @stream, @state, EventsToSync.new([]))

      assert {:error, %MemoryStore.NotCheckedOut{}} = MemoryStore.inspect("")
    end

    test "can only be done once per process" do
      self = self()
      assert :ok = MemoryStore.checkout()
      assert {:error, {:already_checked_out, ^self}} = MemoryStore.checkout()
    end
  end

  describe "sync/3" do
    setup do
      MemoryStore.checkout()
    end

    test "encodes and appends events to a stream, folds unencoded events into a new state, returns it" do
      store = MemoryStore.new(codec: NumberCodec, fold: SumFold)

      assert {:ok, %State{value: 3, version: 1}} =
               Store.sync(store, @stream, @state, EventsToSync.new([1, 2]))
    end

    test "properly syncs to non-empty stream with non-conflict version" do
      store = MemoryStore.new(codec: NumberCodec, fold: SumFold)

      assert {:ok, new_state} =
               Store.sync(store, @stream, @state, EventsToSync.new([1, 2]))

      assert {:ok, %State{value: 10, version: 3}} =
               Store.sync(store, @stream, new_state, EventsToSync.new([3, 4]))
    end

    test "handles sync conflicts by returning conflict status with resync function" do
      store = MemoryStore.new(codec: NumberCodec, fold: SumFold)

      assert {:conflict, resync} =
               Store.sync(store, @stream, %{@state | version: 99}, EventsToSync.new(1))

      assert {:ok, %State{value: 0, version: -1} = new_state} = resync.()

      assert {:ok, %State{value: 1, version: 0} = new_state} =
               Store.sync(store, @stream, new_state, EventsToSync.new(1))

      assert {:conflict, resync} =
               Store.sync(store, @stream, %{new_state | version: 99}, EventsToSync.new(3))

      assert {:ok, %State{value: 1, version: 0}} = resync.()
    end

    test "streams are maintained independently from each other" do
      store = MemoryStore.new(codec: NumberCodec, fold: SumFold)

      assert {:ok, %State{version: 0}} =
               Store.sync(
                 store,
                 StreamName.decode!("Invoice-1", 1),
                 @state,
                 EventsToSync.new(1)
               )

      assert {:ok, %State{version: 0}} =
               Store.sync(
                 store,
                 StreamName.decode!("Invoice-2", 1),
                 @state,
                 EventsToSync.new(1)
               )
    end
  end

  describe "load/4" do
    setup do
      MemoryStore.checkout()
    end

    test "loads from empty state" do
      store = MemoryStore.new(codec: NumberCodec, fold: SumFold)

      assert {:ok, %State{value: 0, version: -1}} =
               Store.load(store, @stream, LoadPolicy.new(:default))
    end

    test "loads from non-empty state" do
      store = MemoryStore.new(codec: NumberCodec, fold: SumFold)

      {:ok, _} =
        Store.sync(store, @stream, @state, EventsToSync.new([1, 2]))

      assert {:ok, %State{value: 3, version: 1}} =
               Store.load(store, @stream, LoadPolicy.new(:default))
    end

    test "respects assumes_empty part of the load policy" do
      store = MemoryStore.new(codec: NumberCodec, fold: SumFold)

      {:ok, _} =
        Store.sync(store, @stream, @state, EventsToSync.new([1, 2]))

      assert {:ok, %State{value: 0, version: -1}} =
               Store.load(store, @stream, LoadPolicy.new(:assume_empty))
    end
  end

  describe "isolation" do
    test "different checkouts interact with isolated stores (and streams)" do
      tasks = [
        Task.async(fn ->
          MemoryStore.checkout()
          store = MemoryStore.new(codec: NumberCodec, fold: SumFold)

          {:ok, _} =
            Store.sync(store, @stream, @state, EventsToSync.new([1, 2]))

          Process.sleep(100)

          assert {:ok, %State{value: 3, version: 1}} =
                   Store.load(store, @stream, LoadPolicy.new(:default))
        end),
        Task.async(fn ->
          MemoryStore.checkout()
          store = MemoryStore.new(codec: NumberCodec, fold: SumFold)

          {:ok, _} =
            Store.sync(store, @stream, @state, EventsToSync.new([1, 2]))

          Process.sleep(100)

          assert {:ok, %State{value: 3, version: 1}} =
                   Store.load(store, @stream, LoadPolicy.new(:default))
        end)
      ]

      Enum.each(tasks, &Task.await/1)
    end

    test "store is automatically removed when owner process goes down" do
      Task.async(fn ->
        MemoryStore.checkout()
        store = MemoryStore.new(codec: NumberCodec, fold: SumFold)

        {:ok, _} =
          Store.sync(store, @stream, @state, EventsToSync.new([1, 2]))

        Process.sleep(100)

        assert {:ok, %State{value: 3, version: 1}} =
                 Store.load(store, @stream, LoadPolicy.new(:default))
      end)
      |> Task.await()

      assert :sys.get_state({:global, MemoryStore}).stores == %{}
    end
  end

  describe "inspect/2" do
    @category "Invoice"
    @stream_1 StreamName.decode!("#{@category}-1", 1)
    @stream_2 StreamName.decode!("#{@category}-2", 1)

    setup do
      MemoryStore.checkout()
      store = MemoryStore.new(codec: NumberCodec, fold: SumFold)
      Store.sync(store, @stream_1, @state, EventsToSync.new([1, 2, 3]))
      Store.sync(store, @stream_2, @state, EventsToSync.new([3, 4, 5]))
      :ok
    end

    test "allows inspecting all timeline events written under certain stream" do
      assert {:ok,
              %{
                @stream_1 => [
                  %TimelineEvent{data: 1, position: 0},
                  %TimelineEvent{data: 2, position: 1},
                  %TimelineEvent{data: 3, position: 2}
                ]
              }} = MemoryStore.inspect(@stream_1)

      assert {:ok,
              %{
                @stream_2 => [
                  %TimelineEvent{data: 3, position: 0},
                  %TimelineEvent{data: 4, position: 1},
                  %TimelineEvent{data: 5, position: 2}
                ]
              }} = MemoryStore.inspect(@stream_2.whole)

      assert MemoryStore.inspect(StreamName.decode!("Payroll-3", 1)) == {:ok, %{}}
    end

    test "allows inspecting all timeline events written under certain category" do
      assert {:ok,
              %{
                @stream_1 => [
                  %TimelineEvent{data: 1, position: 0},
                  %TimelineEvent{data: 2, position: 1},
                  %TimelineEvent{data: 3, position: 2}
                ],
                @stream_2 => [
                  %TimelineEvent{data: 3, position: 0},
                  %TimelineEvent{data: 4, position: 1},
                  %TimelineEvent{data: 5, position: 2}
                ]
              }} = MemoryStore.inspect(@category)

      assert MemoryStore.inspect("Payroll") == {:ok, %{}}
    end
  end

  describe "listeners" do
    test "are sent new timeline events when they are being written" do
      MemoryStore.checkout()
      MemoryStore.add_listener(self())
      store = MemoryStore.new(codec: NumberCodec, fold: SumFold)
      Store.sync(store, @stream, @state, EventsToSync.new([1, 2]))

      assert_receive %TimelineEvent{data: 1}
      assert_receive %TimelineEvent{data: 2}
    end

    test "are cleared when owner process shuts down" do
      Task.async(fn ->
        MemoryStore.checkout()
        MemoryStore.add_listener(self())
      end)
      |> Task.await()

      assert :sys.get_state({:global, MemoryStore}).listeners == %{}
    end
  end
end
