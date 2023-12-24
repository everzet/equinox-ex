defmodule Equinox.StatefulDeciderTest do
  use ExUnit.Case, async: false

  import Mox
  import ExUnit.CaptureLog

  alias Equinox.Stream.StreamName
  alias Equinox.{Decider, State, Lifetime}
  alias Equinox.TestMocks.{StoreMock, CodecMock, FoldMock, LifetimeMock}

  setup :verify_on_exit!

  describe "supervisor" do
    test "restarts process (which reloads state from store) when it crashes" do
      start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: DeciderTestSupervisor})
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      decider = build_decider(supervisor: DeciderTestSupervisor, registry: DeciderTestRegistry)

      expect(FoldMock, :initial, 2, fn -> :initial end)
      expect(StoreMock, :load!, 2, fn _, _, _, _ -> State.new(0, -1) end)

      {:ok, initial_pid} = Decider.Stateful.start_server(decider)
      assert initial_pid == GenServer.whereis(decider.server_name)

      capture_exit(fn -> Decider.transact(initial_pid, fn _ -> raise RuntimeError end) end)
      refute Process.alive?(initial_pid)

      new_pid = GenServer.whereis(decider.server_name)
      assert Process.alive?(new_pid)
      assert new_pid != initial_pid
    end

    test "does not restart processes that exited due to lifetime timeout" do
      start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: DeciderTestSupervisor})
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      decider =
        build_decider(
          supervisor: DeciderTestSupervisor,
          registry: DeciderTestRegistry,
          lifetime: LifetimeMock
        )

      expect(FoldMock, :initial, fn -> :initial end)
      expect(StoreMock, :load!, 1, fn _, %{version: -1}, _, _ -> State.new(0, -1) end)
      expect(LifetimeMock, :after_init, fn _ -> 0 end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)

      Process.sleep(100)

      refute Process.alive?(pid)
      assert GenServer.whereis(decider.server_name) == nil
    end
  end

  describe "registry" do
    test "allows interacting with process without carrying pid around" do
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      stream = "Invoice-1"

      decider =
        build_decider(stream_name: StreamName.parse!(stream), registry: DeciderTestRegistry)

      stub(FoldMock, :initial, fn -> 0 end)

      expect(StoreMock, :load!, fn ^stream, %{version: -1}, _, _ -> State.new(0, -1) end)
      expect(StoreMock, :sync!, fn ^stream, %{version: -1}, [2], _, _, _ -> State.new(2, 0) end)
      expect(StoreMock, :sync!, fn ^stream, %{version: 0}, [3], _, _, _ -> State.new(5, 1) end)

      assert {:ok, ^decider} = Decider.transact(decider, fn 0 -> 2 end)
      assert {:ok, ^decider} = Decider.transact(decider, fn 2 -> 3 end)
      assert 5 = Decider.query(decider, & &1)
    end

    test "isolates processes by stream name" do
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      stub(FoldMock, :initial, fn -> 0 end)

      stream_1 = "Invoice-1"
      stream_2 = "Invoice-2"

      expect(StoreMock, :load!, 2, fn _, %{version: -1}, _, _ -> State.new(0, -1) end)
      expect(StoreMock, :sync!, fn ^stream_1, %{version: -1}, [2], _, _, _ -> State.new(2, 0) end)
      expect(StoreMock, :sync!, fn ^stream_2, %{version: -1}, [3], _, _, _ -> State.new(3, 0) end)

      decider_1 =
        build_decider(stream_name: StreamName.parse!(stream_1), registry: DeciderTestRegistry)

      assert {:ok, ^decider_1} = Decider.transact(decider_1, fn 0 -> [2] end)

      decider_2 =
        build_decider(stream_name: StreamName.parse!(stream_2), registry: DeciderTestRegistry)

      assert {:ok, ^decider_2} = Decider.transact(decider_2, fn 0 -> [3] end)

      assert 2 = Decider.query(decider_1, & &1)
      assert 3 = Decider.query(decider_2, & &1)
    end
  end

  describe "lifetime" do
    test "after_init lifetime controls how long process will wait for query or transact until shutting down" do
      decider = build_decider(lifetime: LifetimeMock)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :load!, fn _, %{version: -1}, _, _ -> State.new(0, -1) end)

      expect(LifetimeMock, :after_init, fn _ -> 0 end)
      {:ok, pid} = Decider.Stateful.start_server(decider)
      Process.sleep(100)
      refute Process.alive?(pid)

      expect(LifetimeMock, :after_init, fn _ -> :timer.seconds(10) end)
      {:ok, pid} = Decider.Stateful.start_server(decider)
      Process.sleep(100)
      assert Process.alive?(pid)
    end

    test "after_query lifetime controls how long process will wait for another query or transact until shutting down" do
      decider = build_decider(lifetime: LifetimeMock)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :load!, fn _, %{version: -1}, _, _ -> State.new(0, -1) end)
      stub(LifetimeMock, :after_init, fn _ -> :timer.seconds(10) end)

      expect(LifetimeMock, :after_query, fn _ -> 0 end)
      {:ok, pid} = Decider.Stateful.start_server(decider)
      0 = Decider.query(pid, & &1)
      Process.sleep(100)
      refute Process.alive?(pid)

      expect(LifetimeMock, :after_query, fn _ -> :timer.seconds(10) end)
      {:ok, pid} = Decider.Stateful.start_server(decider)
      0 = Decider.query(pid, & &1)
      Process.sleep(100)
      assert Process.alive?(pid)
    end

    test "after_transact lifetime controls how long process will wait for another query or transact until shutting down" do
      decider = build_decider(lifetime: LifetimeMock)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :load!, fn _, %{version: -1}, _, _ -> State.new(0, -1) end)
      stub(LifetimeMock, :after_init, fn _ -> :timer.seconds(10) end)

      expect(LifetimeMock, :after_transact, fn _ -> 0 end)
      {:ok, pid} = Decider.Stateful.start_server(decider)
      {:ok, ^pid} = Decider.transact(pid, fn _ -> nil end)
      Process.sleep(100)
      refute Process.alive?(pid)

      expect(LifetimeMock, :after_transact, fn _ -> :timer.seconds(10) end)
      {:ok, pid} = Decider.Stateful.start_server(decider)
      {:ok, ^pid} = Decider.transact(pid, fn _ -> nil end)
      Process.sleep(100)
      assert Process.alive?(pid)
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

  defp build_decider(attrs) do
    test_pid = self()

    Decider.Stateful.for_stream(
      Keyword.get(attrs, :stream_name, StreamName.parse!("Invoice-1")),
      supervisor: Keyword.get(attrs, :supervisor, :disabled),
      registry: Keyword.get(attrs, :registry, :disabled),
      lifetime: Keyword.get(attrs, :lifetime, Lifetime.StayAliveFor30Seconds),
      store: StoreMock,
      codec: CodecMock,
      fold: FoldMock,
      opts: [
        max_load_attempts: Keyword.get(attrs, :max_load_attempts, 2),
        max_sync_attempts: Keyword.get(attrs, :max_sync_attempts, 2),
        max_resync_attempts: Keyword.get(attrs, :max_resync_attempts, 1),
        on_init: fn ->
          allow(LifetimeMock, test_pid, self())
          allow(StoreMock, test_pid, self())
          allow(CodecMock, test_pid, self())
          allow(FoldMock, test_pid, self())
        end
      ]
    )
  end
end
