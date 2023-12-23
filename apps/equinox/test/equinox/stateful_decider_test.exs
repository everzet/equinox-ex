defmodule Equinox.StatefulDeciderTest do
  use ExUnit.Case, async: true

  import Mox
  import ExUnit.CaptureLog

  alias Equinox.TestMocks.{StoreMock, CodecMock, FoldMock, LifetimeMock}
  alias Equinox.Events.{TimelineEvent, EventData}
  alias Equinox.Store.StreamVersionConflict
  alias Equinox.Stream.StreamName
  alias Equinox.{Decider, Lifetime, UUID}

  setup :verify_on_exit!

  describe "start_server/1" do
    test "starts decider process with initial state for brand new streams" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> :initial end)
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)

      assert :initial = Decider.query(pid, & &1)
    end

    test "folds stored events into latest state for existing streams" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 1 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :decode, &{:ok, &1.data})

      expect(StoreMock, :fetch_timeline_events, fn ^stream, -1 ->
        [
          build(:timeline_event, data: 1),
          build(:timeline_event, data: 2),
          build(:timeline_event, data: 3)
        ]
      end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)
      assert 7 = Decider.query(pid, & &1)
    end

    test "gracefully handles fetch failures by retrying certain number of attempts" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream, max_load_attempts: 3)

      stub(FoldMock, :initial, fn -> :initial end)

      expect(StoreMock, :fetch_timeline_events, 2, fn ^stream, -1 -> raise RuntimeError end)
      expect(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)
      assert :initial = Decider.query(pid, & &1)
      refute_receive {:EXIT, ^pid, _}
    end

    test "respects max_load_attempts setting when retrying" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream, max_load_attempts: 2)

      stub(FoldMock, :initial, fn -> :initial end)

      expect(StoreMock, :fetch_timeline_events, 2, fn ^stream, -1 -> raise RuntimeError end)

      assert capture_exit(fn -> Decider.Stateful.start_server(decider) end) =~ "RuntimeError"
    end

    test "codec errors do not trigger retries and instead just crash the process" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream, max_load_attempts: 3)

      stub(FoldMock, :initial, fn -> :initial end)
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [build(:timeline_event)] end)

      expect(CodecMock, :decode, fn _ -> raise RuntimeError end)

      assert capture_exit(fn -> Decider.Stateful.start_server(decider) end) =~ "CodecError"
    end

    test "fold errors do not trigger retries and instead just crash the process" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream, max_load_attempts: 3)

      stub(FoldMock, :initial, fn -> :initial end)
      stub(CodecMock, :decode, &{:ok, &1.data})
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [build(:timeline_event)] end)

      expect(FoldMock, :evolve, fn _, _ -> raise RuntimeError end)

      assert capture_exit(fn -> Decider.Stateful.start_server(decider) end) =~ "FoldError"
    end
  end

  describe "query/2" do
    test "allows to query decider state via provided callback function" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> :some_value end)
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)
      assert :some_value = Decider.query(pid, & &1)
    end

    test "query callback errors are not captured and do crash the process" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> :initial end)
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)

      assert capture_exit(fn -> Decider.query(pid, fn _ -> raise RuntimeError end) end) =~
               "RuntimeError"
    end
  end

  describe "transact/3" do
    test "executes decision callback, writes events it produced and folds them back into state" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, :context -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

      expect(StoreMock, :write_event_data, fn ^stream, events, -1 ->
        assert [%EventData{data: 2}, %EventData{data: 3}] = events
        {:ok, 1}
      end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)
      assert {:ok, ^pid} = Decider.transact(pid, fn 0 -> [2, 3] end, :context)
      assert 5 = Decider.query(pid, & &1)
    end

    test "decision callback returning nil or empty list does nothing" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)
      assert {:ok, ^pid} = Decider.transact(pid, fn 0 -> nil end)
      assert {:ok, ^pid} = Decider.transact(pid, fn 0 -> [] end)
    end

    test "decision callback returning error simply propagates it back" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)
      assert {:error, :custom_error} = Decider.transact(pid, fn 0 -> {:error, :custom_error} end)
    end

    test "decision callback exceptions are not captured and do crash the process" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)

      assert capture_exit(fn -> Decider.transact(pid, fn _ -> raise RuntimeError end) end) =~
               "RuntimeError"
    end

    test "respects previous events" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})

      expect(StoreMock, :fetch_timeline_events, fn ^stream, -1 ->
        [build(:timeline_event, data: 2, position: 0)]
      end)

      expect(StoreMock, :write_event_data, fn ^stream, events, 0 ->
        assert [%EventData{data: 3}] = events
        {:ok, 1}
      end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)
      assert {:ok, ^pid} = Decider.transact(pid, fn 2 -> 3 end)
      assert 5 = Decider.query(pid, & &1)
    end

    test "gracefully handles expected version conflict by reloading the state and redoing the decision" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})

      expect(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

      expect(StoreMock, :write_event_data, fn ^stream, _, -1 ->
        {:error, %StreamVersionConflict{}}
      end)

      expect(StoreMock, :fetch_timeline_events, fn ^stream, -1 ->
        [build(:timeline_event, data: 2, position: 0)]
      end)

      expect(StoreMock, :write_event_data, fn ^stream, events, 0 -> {:ok, length(events)} end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)
      assert {:ok, ^pid} = Decider.transact(pid, fn _ -> 3 end)
      assert 5 = Decider.query(pid, & &1)
    end

    test "respects max_resync_attempts setting when redoing the decision" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream, max_resync_attempts: 0)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

      expect(StoreMock, :write_event_data, fn ^stream, _, -1 ->
        {:error, %StreamVersionConflict{}}
      end)

      {:ok, pid} = Decider.Stateful.start_server(decider)

      assert capture_exit(fn -> Decider.transact(pid, & &1) end) =~ "StreamVersionConflict"
    end

    test "gracefully handles general event write errors by retrying certain number of attempts" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})

      expect(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)
      expect(StoreMock, :write_event_data, 2, fn ^stream, _, -1 -> raise RuntimeError end)
      expect(StoreMock, :write_event_data, fn ^stream, events, -1 -> {:ok, length(events)} end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)
      assert {:ok, ^pid} = Decider.transact(pid, fn _ -> 3 end)
      assert 3 = Decider.query(pid, & &1)
    end

    test "respects max_write_attempts setting when redoing the writes" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream, max_write_attempts: 1)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})

      expect(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)
      expect(StoreMock, :write_event_data, fn ^stream, _, -1 -> raise RuntimeError end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)

      assert capture_exit(fn -> Decider.transact(pid, & &1) end) =~ "RuntimeError"
    end

    test "fold errors do not trigger retries" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})

      stub(FoldMock, :evolve, fn _, _ -> raise RuntimeError end)
      expect(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)
      expect(StoreMock, :write_event_data, fn ^stream, events, -1 -> {:ok, length(events)} end)

      assert {:ok, pid} = Decider.Stateful.start_server(decider)

      assert capture_exit(fn -> Decider.transact(pid, & &1) end) =~ "FoldError"
    end
  end

  describe "supervisor" do
    test "restarts process (which reloads state from store) when it crashes" do
      start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: DeciderTestSupervisor})
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      decider =
        build(:decider,
          stream_name: StreamName.parse!("Invoice-1"),
          supervisor: DeciderTestSupervisor,
          registry: DeciderTestRegistry
        )

      expect(FoldMock, :initial, 2, fn -> :initial end)
      expect(StoreMock, :fetch_timeline_events, 2, fn _, -1 -> [] end)

      {:ok, initial_pid} = Decider.Stateful.start_server(decider)
      assert initial_pid == GenServer.whereis(decider.process_name)

      capture_exit(fn -> Decider.transact(initial_pid, fn _ -> raise RuntimeError end) end)
      refute Process.alive?(initial_pid)

      new_pid = GenServer.whereis(decider.process_name)
      assert Process.alive?(new_pid)
      assert new_pid != initial_pid
    end

    test "does not restart processes that exited due to lifetime timeout" do
      start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: DeciderTestSupervisor})
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      decider =
        build(:decider,
          stream_name: StreamName.parse!("Invoice-1"),
          supervisor: DeciderTestSupervisor,
          registry: DeciderTestRegistry,
          lifetime: LifetimeMock
        )

      expect(FoldMock, :initial, fn -> :initial end)
      expect(StoreMock, :fetch_timeline_events, fn _, -1 -> [] end)
      expect(LifetimeMock, :after_init, fn _ -> 0 end)

      {:ok, pid} = Decider.Stateful.start_server(decider)
      assert pid == GenServer.whereis(decider.process_name)

      Process.sleep(100)

      refute Process.alive?(pid)
      assert GenServer.whereis(decider.process_name) == nil
    end
  end

  describe "registry" do
    test "allows interacting with process without carrying pid around" do
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream, registry: DeciderTestRegistry)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, nil -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

      stub(StoreMock, :write_event_data, fn ^stream, events, idx ->
        {:ok, idx + length(events)}
      end)

      assert {:ok, ^decider} = Decider.transact(decider, fn 0 -> [2] end)
      assert {:ok, ^decider} = Decider.transact(decider, fn 2 -> [3] end)
      assert 5 = Decider.query(decider, & &1)
    end

    test "isolates processes by stream name" do
      start_supervised!({Registry, keys: :unique, name: DeciderTestRegistry})

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, nil -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})
      stub(StoreMock, :fetch_timeline_events, fn _, -1 -> [] end)
      stub(StoreMock, :write_event_data, fn _, events, idx -> {:ok, idx + length(events)} end)

      stream_1 = StreamName.parse!("Invoice-1")
      decider_1 = build(:decider, stream_name: stream_1, registry: DeciderTestRegistry)
      assert {:ok, ^decider_1} = Decider.transact(decider_1, fn 0 -> [2] end)

      stream_2 = StreamName.parse!("Invoice-2")
      decider_2 = build(:decider, stream_name: stream_2, registry: DeciderTestRegistry)
      assert {:ok, ^decider_2} = Decider.transact(decider_2, fn 0 -> [3] end)

      assert 2 = Decider.query(decider_1, & &1)
      assert 3 = Decider.query(decider_2, & &1)
    end
  end

  describe "lifetime" do
    test "after_init lifetime controls how long process will wait for query or transact until shutting down" do
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream, lifetime: LifetimeMock)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)

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
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream, lifetime: LifetimeMock)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)
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
      stream = StreamName.parse!("Invoice-1")
      decider = build(:decider, stream_name: stream, lifetime: LifetimeMock)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :fetch_timeline_events, fn ^stream, -1 -> [] end)
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

  defp build(fixture, attrs \\ [])

  defp build(:decider, attrs) do
    test_pid = self()

    Decider.Stateful.for_stream(
      Keyword.get(attrs, :stream_name, StreamName.parse!("Invoice-1")),
      supervisor: Keyword.get(attrs, :supervisor, :disabled),
      registry: Keyword.get(attrs, :registry, :disabled),
      lifetime: Keyword.get(attrs, :lifetime, Lifetime.Default),
      store: StoreMock,
      codec: CodecMock,
      fold: FoldMock,
      opts: [
        max_load_attempts: Keyword.get(attrs, :max_load_attempts, 3),
        max_write_attempts: Keyword.get(attrs, :max_write_attempts, 3),
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

  defp build(:event_data, attrs) do
    EventData.new(
      id: Keyword.get(attrs, :id, UUID.generate()),
      type: Keyword.get(attrs, :type, "TestEvent"),
      data: Keyword.get(attrs, :data),
      metadata: Keyword.get(attrs, :metadata)
    )
  end

  defp build(:timeline_event, attrs) do
    TimelineEvent.new(
      id: Keyword.get(attrs, :id, UUID.generate()),
      type: Keyword.get(attrs, :type, "TestEvent"),
      stream_name: Keyword.get(attrs, :stream_name, "TestStream"),
      position: Keyword.get(attrs, :position, 0),
      global_position: Keyword.get(attrs, :global_position, 0),
      data: Keyword.get(attrs, :data),
      metadata: Keyword.get(attrs, :metadata),
      time: NaiveDateTime.utc_now()
    )
  end
end
