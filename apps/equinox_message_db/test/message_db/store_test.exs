defmodule Equinox.MessageDb.StoreTest do
  use Equinox.MessageDb.ConnCase

  alias Equinox.State
  alias Equinox.Events.EventData
  alias Equinox.MessageDb.Store.{Unoptimized, LatestKnownEvent}

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

  describe "Unoptimized" do
    test_in_isolation "syncs and loads state by writing and fetching all events", %{conn: conn} do
      state = State.init(SumFold)

      assert %State{value: 5, version: 1} =
               Unoptimized.sync!(conn, "num-1", state, [2, 3], :ctx, NumberCodec, SumFold)

      assert %State{value: 9, version: 2} =
               Unoptimized.sync!(conn, "num-1", State.new(5, 1), [4], :ctx, NumberCodec, SumFold)

      assert %State{value: 9, version: 2} =
               Unoptimized.load!(conn, "num-1", state, NumberCodec, SumFold)

      assert %State{value: 9, version: 2} =
               Unoptimized.load!(conn, "num-1", State.new(5, 1), NumberCodec, SumFold)
    end
  end

  describe "LatestKnownEvent" do
    test_in_isolation "syncs and loads state by writing and fetching all events", %{conn: conn} do
      state = State.init(SumFold)

      assert %State{value: 4, version: 2} =
               LatestKnownEvent.sync!(conn, "num-1", state, [2, 3, 4], :ctx, NumberCodec, InsFold)

      assert %State{value: 4, version: 2} =
               LatestKnownEvent.load!(conn, "num-1", state, NumberCodec, InsFold)

      assert %State{value: 4, version: 2} =
               LatestKnownEvent.load!(conn, "num-1", State.new(3, 1), NumberCodec, InsFold)
    end
  end
end
