defmodule Equinox.Decider.AsyncTest do
  use ExUnit.Case, async: false

  import Mox
  import ExUnit.CaptureLog

  alias Equinox.{Decider, Lifetime}
  alias Equinox.Store.State
  alias Equinox.TestMocks.{StoreMock, LifetimeMock}

  setup :verify_on_exit!

  describe "start/1" do
    test "spawns server and returns pid if registry is :disabled" do
      async = init(registry: :disabled)
      async = Decider.Async.start(async)
      assert is_pid(async.server_name)
    end

    test "spawns different servers every time if registry is :disabled" do
      async = init(registry: :disabled)
      pid1 = Decider.Async.start(async)
      pid2 = Decider.Async.start(async)
      assert pid1 != pid2
    end

    test "spawns server and returns the argument if registry is not :disabled" do
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})
      async = init(registry: DeciderTestRegistry)
      assert ^async = Decider.Async.start(async)
    end

    test "returns the argument if registry is not :disabled" do
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})
      async = init(registry: DeciderTestRegistry)
      assert ^async = Decider.Async.start(async)
    end

    test "keeps single server running if registry is not :disabled" do
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})
      async = init(registry: DeciderTestRegistry)

      assert ^async = Decider.Async.start(async)
      pid = GenServer.whereis(async.server_name)

      assert ^async = Decider.Async.start(async)
      assert GenServer.whereis(async.server_name) == pid
    end

    test "only loads state on boot if it was not loaded already" do
      async = init()

      expect(StoreMock, :load, 0, fn _, _, _, _ -> raise RuntimeError end)

      assert async =
               put_in(async.decider.state, State.new(:value, 2))
               |> Decider.Async.start()

      assert {:value, ^async} = Decider.query(async, & &1)
    end
  end

  test "async context overrides initial decider context" do
    async = init()
    async = put_in(async.decider.context, %{value: :a})
    async = put_in(async.context, %{value: :b})

    stub(StoreMock, :load, fn _, _, _, _ -> {:ok, State.new(0, -1)} end)

    expect(StoreMock, :sync, fn _, state, %{context: %{value: :b}}, _ -> {:ok, state} end)

    async = Decider.Async.start(async)
    Decider.transact(async, fn _ -> [1] end)
  end

  describe "supervisor" do
    test "restarts process (which reloads state from store) when it crashes" do
      start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: DeciderTestSupervisor})
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      async = init(supervisor: DeciderTestSupervisor, registry: DeciderTestRegistry)

      expect(StoreMock, :load, 2, fn _, _, _, _ -> {:ok, State.new(0, -1)} end)

      async = Decider.Async.start(async)
      initial_pid = GenServer.whereis(async.server_name)
      assert Process.alive?(initial_pid)

      capture_exit(fn -> Decider.transact(async, fn _ -> raise RuntimeError end) end)
      refute Process.alive?(initial_pid)

      Process.sleep(50)

      new_pid = GenServer.whereis(async.server_name)
      assert Process.alive?(new_pid)
      assert new_pid != initial_pid
    end

    test "does not restart processes that exited due to lifetime timeout" do
      start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: DeciderTestSupervisor})
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      async =
        init(
          supervisor: DeciderTestSupervisor,
          registry: DeciderTestRegistry,
          lifetime: LifetimeMock
        )

      expect(StoreMock, :load, 1, fn _, nil, _, _ -> {:ok, State.new(0, -1)} end)
      expect(LifetimeMock, :after_init, 1, fn _ -> 0 end)

      {:ok, pid} = Decider.Async.start_server(async)

      Process.sleep(50)

      refute Process.alive?(pid)
      refute GenServer.whereis(async.server_name)
    end
  end

  describe "registry" do
    test "allows interacting with process without carrying pid around" do
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      stream = "Invoice-1"
      async = init(stream_name: stream, registry: DeciderTestRegistry)

      stub(StoreMock, :load, fn ^stream, nil, _, _ -> {:ok, State.new(0, -1)} end)

      expect(StoreMock, :sync, fn ^stream, %{version: -1}, %{events: [2]}, _ ->
        {:ok, State.new(2, 0)}
      end)

      expect(StoreMock, :sync, fn ^stream, %{version: 0}, %{events: [3]}, _ ->
        {:ok, State.new(5, 1)}
      end)

      assert {:ok, ^async} = Decider.transact(async, fn 0 -> 2 end)
      assert {:ok, ^async} = Decider.transact(async, fn 2 -> 3 end)
      assert {5, ^async} = Decider.query(async, & &1)
    end

    test "isolates processes by stream name" do
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      stream_1 = "Invoice-1"
      decider_1 = init(stream_name: stream_1, registry: DeciderTestRegistry)
      expect(StoreMock, :load, fn ^stream_1, nil, _, _ -> {:ok, State.new(0, -1)} end)

      expect(StoreMock, :sync, fn ^stream_1, %{version: -1}, %{events: [2]}, _ ->
        {:ok, State.new(2, 0)}
      end)

      stream_2 = "Invoice-2"
      decider_2 = init(stream_name: stream_2, registry: DeciderTestRegistry)
      expect(StoreMock, :load, fn ^stream_2, nil, _, _ -> {:ok, State.new(0, -1)} end)

      expect(StoreMock, :sync, fn ^stream_2, %{version: -1}, %{events: [3]}, _ ->
        {:ok, State.new(3, 0)}
      end)

      assert {:ok, ^decider_1} = Decider.transact(decider_1, fn 0 -> [2] end)
      assert {2, ^decider_1} = Decider.query(decider_1, & &1)

      assert {:ok, ^decider_2} = Decider.transact(decider_2, fn 0 -> [3] end)
      assert {3, ^decider_2} = Decider.query(decider_2, & &1)
    end

    test ":global is supported" do
      stream = "Invoice-1"
      async = init(stream_name: stream, registry: :global)

      stub(StoreMock, :load, fn _, nil, _, _ -> {:ok, State.new(5, -1)} end)

      assert {5, ^async} = Decider.query(async, & &1)

      assert pid = GenServer.whereis({:global, stream})
      assert Process.alive?(pid)
    end

    test ":global can be prefixed" do
      stream = "Invoice-1"
      async = init(stream_name: stream, registry: {:global, "prefix-"})

      stub(StoreMock, :load, fn _, nil, _, _ -> {:ok, State.new(5, -1)} end)

      assert {5, ^async} = Decider.query(async, & &1)

      assert pid = GenServer.whereis({:global, "prefix-" <> stream})
      assert Process.alive?(pid)
    end
  end

  describe "lifetime" do
    test "after_init lifetime controls how long process will wait for query or transact until shutting down" do
      async = init(lifetime: LifetimeMock)

      stub(StoreMock, :load, fn _, nil, _, _ -> {:ok, State.new(0, -1)} end)

      expect(LifetimeMock, :after_init, fn _ -> 0 end)
      {:ok, pid} = Decider.Async.start_server(async)
      Process.sleep(50)
      refute Process.alive?(pid)

      expect(LifetimeMock, :after_init, fn _ -> :timer.seconds(10) end)
      {:ok, pid} = Decider.Async.start_server(async)
      Process.sleep(50)
      assert Process.alive?(pid)
    end

    test "after_query lifetime controls how long process will wait for another query or transact until shutting down" do
      async = init(lifetime: LifetimeMock)

      stub(StoreMock, :load, fn _, nil, _, _ -> {:ok, State.new(0, -1)} end)
      stub(LifetimeMock, :after_init, fn _ -> :timer.seconds(10) end)

      expect(LifetimeMock, :after_query, fn _ -> 0 end)
      a = Decider.Async.start(async)
      Decider.query(a, & &1)
      Process.sleep(50)
      refute Process.alive?(a.server_name)

      expect(LifetimeMock, :after_query, fn _ -> :timer.seconds(10) end)
      b = Decider.Async.start(async)
      Decider.query(b, & &1)
      Process.sleep(50)
      assert Process.alive?(b.server_name)
    end

    test "after_transact lifetime controls how long process will wait for another query or transact until shutting down" do
      async = init(lifetime: LifetimeMock)

      stub(StoreMock, :load, fn _, nil, _, _ -> {:ok, State.new(0, -1)} end)
      stub(LifetimeMock, :after_init, fn _ -> :timer.seconds(10) end)

      expect(LifetimeMock, :after_transact, fn _ -> 0 end)
      a = Decider.Async.start(async)
      {:ok, ^a} = Decider.transact(a, fn _ -> nil end)
      Process.sleep(50)
      refute Process.alive?(a.server_name)

      expect(LifetimeMock, :after_transact, fn _ -> :timer.seconds(10) end)
      b = Decider.Async.start(async)
      {:ok, ^b} = Decider.transact(b, fn _ -> nil end)
      Process.sleep(50)
      assert Process.alive?(b.server_name)
    end
  end

  defp capture_exit(fun) do
    capture_log(fn ->
      Process.flag(:trap_exit, true)

      try do
        fun.()
        assert_receive {:EXIT, _, _}
      catch
        :exit, _ -> nil
      end
    end)
  end

  defp init(attrs \\ []) do
    attrs
    |> Keyword.get(:stream_name, "Invoice-1")
    |> Decider.async(
      store: {StoreMock, allow_mocks_from: self()},
      supervisor: Keyword.get(attrs, :supervisor, :disabled),
      registry: Keyword.get(attrs, :registry, :disabled),
      lifetime: Keyword.get(attrs, :lifetime, Lifetime.StopAfter30sOfInactivity)
    )
  end
end
