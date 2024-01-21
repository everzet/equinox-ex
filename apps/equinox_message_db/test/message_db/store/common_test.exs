defmodule MessageDb.Store.CommonTest do
  use Equinox.MessageDb.ConnCase

  import Mox

  alias Equinox.Store
  alias Equinox.Store.{State, EventsToSync}
  alias Equinox.Events.EventData
  alias Equinox.Decider.LoadPolicy
  alias Equinox.MessageDb.Store.{Unoptimized, LatestKnownEvent}
  alias Equinox.CacheMock

  defmodule Codec do
    @behaviour Equinox.Codec
    def encode(val, _), do: EventData.new(type: "Test", data: val)
    def decode(event), do: event.data
  end

  defmodule Fold do
    @behaviour Equinox.Fold
    def initial(), do: nil
    def fold(vals, _), do: Enum.at(vals, -1)
  end

  defp new(store_mod, opts) do
    [cache: %CacheMock.Config{}, codec: Codec, fold: Fold]
    |> Keyword.merge(opts)
    |> store_mod.config()
  end

  setup :verify_on_exit!

  # We test all store versions with the same test suite
  Enum.each([Unoptimized, LatestKnownEvent], fn store_mod ->
    @stream "s-1"
    @orig State.init(Fold, -1)

    setup do
      stub(CacheMock, :get, fn @stream, 0 -> nil end)
      stub(CacheMock, :put, fn @stream, %State{} -> :ok end)
      :ok
    end

    describe "#{inspect(store_mod)}.load/3" do
      test_in_isolation "uses the default connection if it's the only one", %{conn: conn} do
        new(unquote(store_mod), conn: conn)
        |> Store.sync(@stream, @orig, EventsToSync.new([2, 3]))

        assert {:ok, %State{value: 3}} =
                 new(unquote(store_mod), conn: conn)
                 |> Store.load(@stream, LoadPolicy.default())
      end

      test_in_isolation "uses follower connection by default", %{conn: conn} do
        new(unquote(store_mod), conn: conn)
        |> Store.sync(@stream, @orig, EventsToSync.new([2, 3]))

        assert {:ok, %State{value: 3}} =
                 new(unquote(store_mod), conn: [follower: conn, leader: :"?"])
                 |> Store.load(@stream, LoadPolicy.default())
      end

      test_in_isolation "uses leader connection if policy requires leader", %{conn: conn} do
        new(unquote(store_mod), conn: conn)
        |> Store.sync(@stream, @orig, EventsToSync.new([2, 3]))

        assert {:ok, %State{value: 3}} =
                 new(unquote(store_mod), conn: [leader: conn, follower: :"?"])
                 |> Store.load(@stream, LoadPolicy.require_leader())
      end

      test_in_isolation "returns empty state without load if policy assumes empty", %{conn: conn} do
        new(unquote(store_mod), conn: conn)
        |> Store.sync(@stream, @orig, EventsToSync.new([2, 3]))

        assert {:ok, %State{value: nil}} =
                 new(unquote(store_mod), conn: conn)
                 |> Store.load(@stream, LoadPolicy.assume_empty())
      end

      test_in_isolation "returns cached value without load if matches policy", %{conn: conn} do
        new(unquote(store_mod), conn: conn)
        |> Store.sync(@stream, @orig, EventsToSync.new([2, 3]))

        expect(CacheMock, :get, fn @stream, 5_000 -> State.new(2, 0) end)

        assert {:ok, %State{value: 2}} =
                 new(unquote(store_mod), conn: conn)
                 |> Store.load(@stream, LoadPolicy.allow_stale(5_000))
      end

      test_in_isolation "always caches loaded value", %{conn: conn} do
        new(unquote(store_mod), conn: conn)
        |> Store.sync(@stream, @orig, EventsToSync.new([2, 3]))

        expect(CacheMock, :put, fn @stream, %State{value: 3} -> :ok end)

        assert {:ok, _} =
                 new(unquote(store_mod), conn: conn)
                 |> Store.load(@stream, LoadPolicy.default())
      end
    end

    describe "#{inspect(store_mod)}.sync/4" do
      test_in_isolation "always uses leader connection", %{conn: conn} do
        {:ok, state} =
          new(unquote(store_mod), conn: conn)
          |> Store.sync(@stream, @orig, EventsToSync.new([2]))

        {:ok, %State{value: 3}} =
          new(unquote(store_mod), conn: [leader: conn, follower: :"?"])
          |> Store.sync(@stream, state, EventsToSync.new([3]))
      end

      test_in_isolation "returns conflict with reload function to use upstream", %{conn: conn} do
        new(unquote(store_mod), conn: conn)
        |> Store.sync(@stream, @orig, EventsToSync.new([2]))

        assert {:conflict, reload_fun} =
                 new(unquote(store_mod), conn: conn)
                 |> Store.sync(@stream, @orig, EventsToSync.new([3]))

        # we are inside a wrapper transaction that is now in a failed state -
        # we roll it back so that we can make a follow-up reload query
        Postgrex.rollback(conn, :end_of_test)

        assert {:ok, %State{value: 2}} = reload_fun.()
      end

      test_in_isolation "stores synced state in cache", %{conn: conn} do
        expect(CacheMock, :put, fn @stream, %State{value: 2} -> :ok end)

        assert {:ok, _} =
                 new(unquote(store_mod), conn: conn)
                 |> Store.sync(@stream, @orig, EventsToSync.new([2]))
      end
    end
  end)
end
