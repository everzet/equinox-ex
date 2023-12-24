defmodule Equinox.MessageDb.StoreTest do
  use Equinox.MessageDb.ConnCase

  alias Equinox.State
  alias Equinox.Events.EventData
  alias Equinox.MessageDb.Store

  defmodule NumberCodec do
    @behaviour Equinox.Codec
    def encode(n, :ctx) when is_number(n), do: {:ok, EventData.new(type: "Number", data: n)}
    def decode(event) when is_number(event.data), do: {:ok, event.data}
  end

  defmodule SumFold do
    @behaviour Equinox.Fold
    def initial(), do: 0
    def evolve(sum, n) when is_number(n), do: sum + n
  end

  defmodule InsFold do
    @behaviour Equinox.Fold
    def initial(), do: nil
    def evolve(_, v), do: v
  end

  describe "sync!/7" do
    @state State.init(SumFold)

    test_in_isolation "syncs from initial state", %{conn: conn} do
      assert %State{value: 9, version: 2} =
               Store.sync!(conn, "num-1", @state, [2, 3, 4], :ctx, NumberCodec, SumFold)
    end

    test_in_isolation "syncs from intermediate state", %{conn: conn} do
      assert %State{value: 5, version: 1} =
               Store.sync!(conn, "num-1", @state, [2, 3], :ctx, NumberCodec, SumFold)

      assert %State{value: 9, version: 2} =
               Store.sync!(conn, "num-1", State.new(5, 1), [4], :ctx, NumberCodec, SumFold)
    end
  end

  describe "load_unoptimized!/6" do
    @state State.init(SumFold)

    test_in_isolation "loads from initial state", %{conn: conn} do
      assert %State{} = Store.sync!(conn, "num-1", @state, [2, 3, 4], :ctx, NumberCodec, SumFold)

      assert %State{value: 9, version: 2} =
               Store.load_unoptimized!(conn, "num-1", @state, NumberCodec, SumFold, 100)
    end

    test_in_isolation "loads from intermediate state", %{conn: conn} do
      assert %State{} = Store.sync!(conn, "num-1", @state, [2, 3, 4], :ctx, NumberCodec, SumFold)

      assert %State{value: 9, version: 2} =
               Store.load_unoptimized!(conn, "num-1", State.new(5, 1), NumberCodec, SumFold, 100)
    end

    test_in_isolation "handles empty streams", %{conn: conn} do
      assert %State{value: 0, version: -1} =
               Store.load_unoptimized!(conn, "num-1", @state, NumberCodec, SumFold, 100)
    end

    test_in_isolation "handles different batch sizes", %{conn: conn} do
      assert %State{} = Store.sync!(conn, "num-1", @state, [2, 3, 4], :ctx, NumberCodec, SumFold)

      assert %State{value: 9, version: 2} =
               Store.load_unoptimized!(conn, "num-1", @state, NumberCodec, SumFold, 1)

      assert %State{value: 9, version: 2} =
               Store.load_unoptimized!(conn, "num-1", @state, NumberCodec, SumFold, 100)
    end
  end

  describe "load_latest_known_event!/5" do
    @state State.init(InsFold)

    test_in_isolation "loads from initial state", %{conn: conn} do
      assert %State{} = Store.sync!(conn, "num-1", @state, [2, 3, 4], :ctx, NumberCodec, InsFold)

      assert %State{value: 4, version: 2} =
               Store.load_latest_known_event!(conn, "num-1", @state, NumberCodec, InsFold)
    end

    test_in_isolation "loads from intermediate state", %{conn: conn} do
      assert %State{} = Store.sync!(conn, "num-1", @state, [2, 3, 4], :ctx, NumberCodec, InsFold)

      assert %State{value: 4, version: 2} =
               Store.load_latest_known_event!(
                 conn,
                 "num-1",
                 State.new(3, 1),
                 NumberCodec,
                 InsFold
               )
    end

    test_in_isolation "handles empty streams", %{conn: conn} do
      assert %State{value: nil, version: -1} =
               Store.load_latest_known_event!(conn, "num-1", @state, NumberCodec, SumFold)
    end
  end
end
