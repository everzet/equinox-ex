defmodule Equinox.Decider.AsyncTest do
  use ExUnit.Case, async: false

  import Mox
  import ExUnit.CaptureLog

  alias Equinox.Decider
  alias Equinox.Codec.StreamName
  alias Equinox.Store.State
  alias Equinox.StoreMock

  setup :verify_on_exit!

  describe "start/1" do
    test "spawns server and returns pid if registry is :disabled" do
      async = init(registry: :disabled)
      async = Decider.Async.start(async)
      assert is_pid(async.server)
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
      pid = GenServer.whereis(async.server)

      assert ^async = Decider.Async.start(async)
      assert GenServer.whereis(async.server) == pid
    end
  end

  describe "supervisor" do
    test "restarts process (which reloads state from store) when it crashes" do
      start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: DeciderTestSupervisor})
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      async = init(supervisor: DeciderTestSupervisor, registry: DeciderTestRegistry)

      async = Decider.Async.start(async)
      initial_pid = GenServer.whereis(async.server)
      assert Process.alive?(initial_pid)

      capture_exit(fn -> Decider.transact(async, fn _ -> raise RuntimeError end) end)
      refute Process.alive?(initial_pid)

      Process.sleep(50)

      new_pid = GenServer.whereis(async.server)
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
          lifetime: [after_init: 0]
        )

      {:ok, pid} = Decider.Async.start_server(async)

      Process.sleep(50)

      refute Process.alive?(pid)
      refute GenServer.whereis(async.server)
    end
  end

  describe "registry" do
    test "allows interacting with process without carrying pid around" do
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      stream = StreamName.decode!("Invoice-1", 1)
      async = init(stream_name: stream, registry: DeciderTestRegistry)

      stub(StoreMock, :load, fn ^stream, _ -> {:ok, State.new(0, -1)} end)

      expect(StoreMock, :sync, fn ^stream, %{version: -1}, %{events: [2]} ->
        {:ok, State.new(2, 0)}
      end)

      assert :ok = Decider.transact(async, fn 0 -> 2 end)
    end

    test "isolates processes by stream name" do
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      stream_1 = StreamName.decode!("Invoice-1", 1)
      decider_1 = init(stream_name: stream_1, registry: DeciderTestRegistry)
      expect(StoreMock, :load, fn ^stream_1, _ -> {:ok, State.new(0, -1)} end)

      expect(StoreMock, :sync, fn ^stream_1, %{version: -1}, %{events: [2]} ->
        {:ok, State.new(2, 0)}
      end)

      stream_2 = StreamName.decode!("Invoice-2", 1)
      decider_2 = init(stream_name: stream_2, registry: DeciderTestRegistry)
      expect(StoreMock, :load, fn ^stream_2, _ -> {:ok, State.new(0, -1)} end)

      expect(StoreMock, :sync, fn ^stream_2, %{version: -1}, %{events: [3]} ->
        {:ok, State.new(3, 0)}
      end)

      assert :ok = Decider.transact(decider_1, fn 0 -> [2] end)
      assert :ok = Decider.transact(decider_2, fn 0 -> [3] end)
    end

    test ":global is supported" do
      stream = StreamName.decode!("Invoice-1", 1)
      async = init(stream_name: stream, registry: :global)

      stub(StoreMock, :load, fn _, _ -> {:ok, State.new(5, -1)} end)

      assert 5 = Decider.query(async, & &1)

      assert pid = GenServer.whereis({:global, stream.combined})
      assert Process.alive?(pid)
    end

    test ":global can be prefixed" do
      stream = StreamName.decode!("Invoice-1", 1)
      async = init(stream_name: stream, registry: {:global, "prefix-"})

      stub(StoreMock, :load, fn _, _ -> {:ok, State.new(5, -1)} end)

      assert 5 = Decider.query(async, & &1)

      assert pid = GenServer.whereis({:global, "prefix-" <> stream.combined})
      assert Process.alive?(pid)
    end
  end

  describe "lifetime" do
    test "after_init lifetime controls how long process will wait for query or transact until shutting down" do
      stub(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

      async = init(lifetime: [after_init: 0])
      {:ok, pid} = Decider.Async.start_server(async)
      Process.sleep(50)
      refute Process.alive?(pid)

      async = init(lifetime: [after_init: 10_000])
      {:ok, pid} = Decider.Async.start_server(async)
      Process.sleep(50)
      assert Process.alive?(pid)
    end

    test "after_query lifetime controls how long process will wait for another query or transact until shutting down" do
      stub(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

      async = init(lifetime: [after_init: 10_000, after_query: 0])
      a = Decider.Async.start(async)
      Decider.query(a, & &1)
      Process.sleep(50)
      refute Process.alive?(a.server)

      async = init(lifetime: [after_init: 10_000, after_query: 10_000])
      b = Decider.Async.start(async)
      Decider.query(b, & &1)
      Process.sleep(50)
      assert Process.alive?(b.server)
    end

    test "after_transact lifetime controls how long process will wait for another query or transact until shutting down" do
      stub(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

      async = init(lifetime: [after_init: 10_000, after_transact: 0])
      async = Decider.Async.start(async)
      :ok = Decider.transact(async, fn _ -> nil end)
      Process.sleep(50)
      refute Process.alive?(async.server)

      async = init(lifetime: [after_init: 10_000, after_transact: 10_000])
      async = Decider.Async.start(async)
      :ok = Decider.transact(async, fn _ -> nil end)
      Process.sleep(50)
      assert Process.alive?(async.server)
    end

    test "lifetime policy can be specified via shortened version (tuple)" do
      init(lifetime: {:max_inactivity, :timer.seconds(10)})
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

  defp init(attrs) do
    attrs
    |> Keyword.get(:stream_name, StreamName.decode!("Invoice-1", 1))
    |> Decider.async(
      store: {StoreMock.Config, allow_from: self()},
      supervisor: Keyword.get(attrs, :supervisor, :disabled),
      registry: Keyword.get(attrs, :registry, :disabled),
      lifetime: Keyword.get(attrs, :lifetime, :default)
    )
  end
end
