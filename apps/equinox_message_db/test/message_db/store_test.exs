defmodule Equinox.MessageDb.StoreTest do
  use Equinox.MessageDb.ConnCase

  alias Equinox.Store.{State, Outcome, StreamVersionConflict}
  alias Equinox.Events.EventData
  alias Equinox.MessageDb.Store

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

  defmodule InsFold do
    @behaviour Equinox.Fold
    def initial(), do: nil
    def fold(vals, _), do: Enum.at(vals, -1)
  end

  describe "sync/6" do
    @fold SumFold
    @codec NumberCodec
    @state State.init(@fold, -1)

    test_in_isolation "syncs from initial state", %{conn: conn} do
      assert {:ok, %State{value: 9, version: 2}} =
               Store.sync(conn, "num-1", @state, Outcome.new([2, 3, 4]), @codec, @fold)
    end

    test_in_isolation "syncs from intermediate state", %{conn: conn} do
      assert {:ok, %State{value: 5, version: 1}} =
               Store.sync(conn, "num-1", @state, Outcome.new([2, 3]), @codec, @fold)

      assert {:ok, %State{value: 9, version: 2}} =
               Store.sync(conn, "num-1", State.new(5, 1), Outcome.new([4]), @codec, @fold)
    end

    test_in_isolation "handles version conflicts by returning error", %{conn: conn} do
      assert {:ok, %State{value: 5, version: 1}} =
               Store.sync(conn, "num-1", @state, Outcome.new([2, 3]), @codec, @fold)

      assert {:error, %StreamVersionConflict{}} =
               Store.sync(conn, "num-1", @state, Outcome.new([4]), @codec, @fold)
    end
  end

  describe "load_unoptimized/6" do
    @fold SumFold
    @codec NumberCodec
    @state State.init(@fold, -1)

    test_in_isolation "loads from non-initialized state", %{conn: conn} do
      assert {:ok, %State{}} =
               Store.sync(conn, "num-1", @state, Outcome.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 9, version: 2}} =
               Store.load_unoptimized(conn, "num-1", nil, @codec, @fold, 100)
    end

    test_in_isolation "loads from initial state", %{conn: conn} do
      assert {:ok, %State{}} =
               Store.sync(conn, "num-1", @state, Outcome.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 9, version: 2}} =
               Store.load_unoptimized(conn, "num-1", @state, @codec, @fold, 100)
    end

    test_in_isolation "loads from intermediate state", %{conn: conn} do
      assert {:ok, %State{}} =
               Store.sync(conn, "num-1", @state, Outcome.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 9, version: 2}} =
               Store.load_unoptimized(conn, "num-1", State.new(5, 1), @codec, @fold, 100)
    end

    test_in_isolation "handles empty streams", %{conn: conn} do
      assert {:ok, %State{value: 0, version: -1}} =
               Store.load_unoptimized(conn, "num-1", @state, @codec, @fold, 100)
    end

    test_in_isolation "handles different batch sizes", %{conn: conn} do
      assert {:ok, %State{}} =
               Store.sync(conn, "num-1", @state, Outcome.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 9, version: 2}} =
               Store.load_unoptimized(conn, "num-1", @state, @codec, @fold, 1)

      assert {:ok, %State{value: 9, version: 2}} =
               Store.load_unoptimized(conn, "num-1", @state, @codec, @fold, 100)
    end

    test_in_isolation "handles errors by returning them and a partial state", %{conn: conn} do
      assert {:error, %Postgrex.Error{}, %State{value: 0, version: -1}} =
               Store.load_unoptimized(conn, nil, @state, @codec, @fold, 100)
    end
  end

  describe "load_latest_known_event/5" do
    @fold InsFold
    @codec NumberCodec
    @state State.init(@fold, -1)

    test_in_isolation "loads from non-initialized state", %{conn: conn} do
      assert {:ok, %State{}} =
               Store.sync(conn, "num-1", @state, Outcome.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 4, version: 2}} =
               Store.load_latest_known_event(conn, "num-1", nil, @codec, @fold)
    end

    test_in_isolation "loads from initial state", %{conn: conn} do
      assert {:ok, %State{}} =
               Store.sync(conn, "num-1", @state, Outcome.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 4, version: 2}} =
               Store.load_latest_known_event(conn, "num-1", @state, @codec, @fold)
    end

    test_in_isolation "loads from intermediate state", %{conn: conn} do
      assert {:ok, %State{}} =
               Store.sync(conn, "num-1", @state, Outcome.new([2, 3, 4]), @codec, @fold)

      assert {:ok, %State{value: 4, version: 2}} =
               Store.load_latest_known_event(conn, "num-1", State.new(3, 1), @codec, @fold)
    end

    test_in_isolation "handles empty streams", %{conn: conn} do
      assert {:ok, %State{value: nil, version: -1}} =
               Store.load_latest_known_event(conn, "num-1", @state, @codec, @fold)
    end
  end
end
