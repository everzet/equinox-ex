defmodule Equinox.MessageDb.Store.BaseTest do
  use Equinox.MessageDb.ConnCase

  alias Equinox.Codec.StreamName
  alias Equinox.Store.{State, EventsToSync}
  alias Equinox.Events.EventData
  alias Equinox.MessageDb.{Store.Base, Writer}

  defmodule NumberCodec do
    @behaviour Equinox.Codec
    def encode(n, %{}), do: EventData.new(type: "Number", data: n)
    def decode(event), do: event.data
  end

  defmodule SumFold do
    @behaviour Equinox.Fold
    def initial, do: 0
    def fold(nums, sum), do: Enum.sum([sum | nums])
  end

  defmodule InsFold do
    @behaviour Equinox.Fold
    def initial, do: nil
    def fold(vals, _), do: Enum.at(vals, -1)
  end

  @stream StreamName.decode!("num-1", 1)

  describe "sync/6" do
    @fold SumFold
    @codec NumberCodec
    @state State.new(@fold.initial(), -1)

    test_in_isolation "syncs from initial state", %{conn: conn} do
      assert {:ok, %State{value: 9, version: 2}} =
               Base.sync(conn, @stream, @state, EventsToSync.new([2, 3, 4]), @codec, @fold)
    end

    test_in_isolation "syncs from intermediate state", %{conn: conn} do
      assert {:ok, %State{value: 5, version: 1}} =
               Base.sync(conn, @stream, @state, EventsToSync.new([2, 3]), @codec, @fold)

      assert {:ok, %State{value: 9, version: 2}} =
               Base.sync(
                 conn,
                 @stream,
                 State.new(5, 1),
                 EventsToSync.new([4]),
                 @codec,
                 @fold
               )
    end

    test_in_isolation "handles version conflicts by returning error", %{conn: conn} do
      assert {:ok, %State{value: 5, version: 1}} =
               Base.sync(conn, @stream, @state, EventsToSync.new([2, 3]), @codec, @fold)

      assert {:error, %Writer.StreamVersionConflictError{}} =
               Base.sync(conn, @stream, @state, EventsToSync.new([4]), @codec, @fold)
    end
  end

  describe "load_unoptimized/6" do
    @fold SumFold
    @codec NumberCodec
    @state State.new(@fold.initial(), -1)

    test_in_isolation "loads from non-initialized state", %{conn: conn} do
      assert {:ok, %State{}} =
               Base.sync(conn, @stream, @state, EventsToSync.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 9, version: 2}} =
               Base.load_unoptimized(conn, @stream, @state, @codec, @fold, 100)
    end

    test_in_isolation "loads from initial state", %{conn: conn} do
      assert {:ok, %State{}} =
               Base.sync(conn, @stream, @state, EventsToSync.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 9, version: 2}} =
               Base.load_unoptimized(conn, @stream, @state, @codec, @fold, 100)
    end

    test_in_isolation "loads from intermediate state", %{conn: conn} do
      assert {:ok, %State{}} =
               Base.sync(conn, @stream, @state, EventsToSync.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 9, version: 2}} =
               Base.load_unoptimized(conn, @stream, State.new(5, 1), @codec, @fold, 100)
    end

    test_in_isolation "handles empty streams", %{conn: conn} do
      assert {:ok, %State{value: 0, version: -1}} =
               Base.load_unoptimized(conn, @stream, @state, @codec, @fold, 100)
    end

    test_in_isolation "handles different batch sizes", %{conn: conn} do
      assert {:ok, %State{}} =
               Base.sync(conn, @stream, @state, EventsToSync.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 9, version: 2}} =
               Base.load_unoptimized(conn, @stream, @state, @codec, @fold, 1)

      assert {:ok, %State{value: 9, version: 2}} =
               Base.load_unoptimized(conn, @stream, @state, @codec, @fold, 100)
    end
  end

  describe "load_latest_known_event/5" do
    @fold InsFold
    @codec NumberCodec
    @state State.new(@fold.initial(), -1)

    test_in_isolation "loads from non-initialized state", %{conn: conn} do
      assert {:ok, %State{}} =
               Base.sync(conn, @stream, @state, EventsToSync.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 4, version: 2}} =
               Base.load_latest_known_event(conn, @stream, @state, @codec, @fold)
    end

    test_in_isolation "loads from initial state", %{conn: conn} do
      assert {:ok, %State{}} =
               Base.sync(conn, @stream, @state, EventsToSync.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 4, version: 2}} =
               Base.load_latest_known_event(conn, @stream, @state, @codec, @fold)
    end

    test_in_isolation "loads from intermediate state", %{conn: conn} do
      assert {:ok, %State{}} =
               Base.sync(conn, @stream, @state, EventsToSync.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 4, version: 2}} =
               Base.load_latest_known_event(conn, @stream, State.new(3, 1), @codec, @fold)
    end

    test_in_isolation "handles empty streams", %{conn: conn} do
      assert {:ok, %State{value: nil, version: -1}} =
               Base.load_latest_known_event(conn, @stream, @state, @codec, @fold)
    end
  end
end
