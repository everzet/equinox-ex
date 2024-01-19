defmodule MessageDb.Store.UnoptimizedTest do
  use Equinox.MessageDb.ConnCase

  import Mox

  alias Equinox.Events.EventData
  alias Equinox.Decider.LoadPolicy
  alias Equinox.MessageDb.Store
  alias Equinox.Store.{State, EventsToSync}
  alias Equinox.TestMocks.CacheMock

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

  @stream "s-1"
  @orig State.init(Fold, -1)
  defp opts(overrides),
    do: Keyword.merge([cache: CacheMock, codec: Codec, fold: Fold], overrides)

  setup :verify_on_exit!

  setup do
    stub(CacheMock, :fetch, fn @stream, 0 -> nil end)
    stub(CacheMock, :insert, fn @stream, %State{} -> :ok end)
    :ok
  end

  # We test all store versions with the same test suite
  Enum.each([Store.Unoptimized, Store.LatestKnownEvent], fn store_mod ->
    describe "#{inspect(store_mod)}.load/3" do
      test_in_isolation "uses follower connection by default", %{conn: conn} do
        policy = LoadPolicy.default()

        {:ok, _} =
          unquote(store_mod).sync(@stream, @orig, EventsToSync.new([2, 3]), opts(conn: conn))

        assert {:ok, %State{value: 3}} =
                 unquote(store_mod).load(@stream, policy, opts(conn: conn))

        assert {:ok, %State{value: 3}} =
                 unquote(store_mod).load(
                   @stream,
                   policy,
                   opts(conn: [follower: conn, leader: :"?"])
                 )
      end

      test_in_isolation "uses leader connection if policy requires leader", %{conn: conn} do
        policy = LoadPolicy.require_leader()

        {:ok, _} =
          unquote(store_mod).sync(@stream, @orig, EventsToSync.new([2, 3]), opts(conn: conn))

        assert {:ok, %State{value: 3}} =
                 unquote(store_mod).load(@stream, policy, opts(conn: conn))

        assert {:ok, %State{value: 3}} =
                 unquote(store_mod).load(
                   @stream,
                   policy,
                   opts(conn: [follower: :"?", leader: conn])
                 )
      end

      test_in_isolation "returns empty state without load if policy assumes empty", %{conn: conn} do
        policy = LoadPolicy.assume_empty()

        {:ok, _} =
          unquote(store_mod).sync(@stream, @orig, EventsToSync.new([2, 3]), opts(conn: conn))

        assert {:ok, %State{value: nil}} =
                 unquote(store_mod).load(@stream, policy, opts(conn: conn))
      end

      test_in_isolation "returns cached value without load if matches policy", %{conn: conn} do
        policy = LoadPolicy.allow_stale(5_000)

        {:ok, _} =
          unquote(store_mod).sync(@stream, @orig, EventsToSync.new([2, 3]), opts(conn: conn))

        expect(CacheMock, :fetch, fn @stream, 5_000 -> State.new(2, 0) end)

        assert {:ok, %State{value: 2}} =
                 unquote(store_mod).load(@stream, policy, opts(conn: conn))
      end

      test_in_isolation "always caches loaded value", %{conn: conn} do
        policy = LoadPolicy.default()

        {:ok, _} =
          unquote(store_mod).sync(@stream, @orig, EventsToSync.new([2, 3]), opts(conn: conn))

        expect(CacheMock, :insert, fn @stream, %State{value: 3} -> :ok end)

        assert {:ok, _} = unquote(store_mod).load(@stream, policy, opts(conn: conn))
      end
    end

    describe "#{inspect(store_mod)}.sync/4" do
      test_in_isolation "always uses leader connection", %{conn: conn} do
        {:ok, state} =
          unquote(store_mod).sync(@stream, @orig, EventsToSync.new([2]), opts(conn: conn))

        {:ok, %State{value: 3}} =
          unquote(store_mod).sync(
            @stream,
            state,
            EventsToSync.new([3]),
            opts(conn: [leader: conn, follower: :"?"])
          )
      end

      test_in_isolation "returns conflict with reload function to use upstream", %{conn: conn} do
        {:ok, _} =
          unquote(store_mod).sync(@stream, @orig, EventsToSync.new([2]), opts(conn: conn))

        assert {:conflict, reload_fun} =
                 unquote(store_mod).sync(@stream, @orig, EventsToSync.new([3]), opts(conn: conn))

        # we are inside a wrapper transaction that is now in a failed state -
        # we roll it back so that we can make a follow-up reload query
        Postgrex.rollback(conn, :end_of_test)

        assert {:ok, %State{value: 2}} = reload_fun.()
      end

      test_in_isolation "stores synced state in cache", %{conn: conn} do
        {:ok, state} =
          unquote(store_mod).sync(@stream, @orig, EventsToSync.new([2]), opts(conn: conn))

        expect(CacheMock, :insert, fn @stream, %State{value: 3} -> :ok end)

        {:ok, _} =
          unquote(store_mod).sync(@stream, state, EventsToSync.new([3]), opts(conn: conn))
      end
    end
  end)
end
