defmodule Equinox.DeciderTest do
  use ExUnit.Case, async: true

  import Mox
  import ExUnit.CaptureLog

  alias Equinox.TestMocks.{StoreMock, CodecMock, FoldMock}
  alias Equinox.Events.{TimelineEvent, EventData}
  alias Equinox.Store.StreamVersionConflict
  alias Equinox.Stream.StreamName
  alias Equinox.Decider
  alias Equinox.UUID

  setup :verify_on_exit!

  describe "start_link/1" do
    test "starts decider process with initial state for brand new streams" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> :initial end)
      stub(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)

      assert :initial = Decider.query(pid, & &1)
    end

    test "folds stored events into latest state for existing streams" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 1 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :decode, &{:ok, &1.data})

      expect(StoreMock, :fetch_events, fn ^stream, -1 ->
        [
          build(:timeline_event, data: 1),
          build(:timeline_event, data: 2),
          build(:timeline_event, data: 3)
        ]
      end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)
      assert 7 = Decider.query(pid, & &1)
    end

    test "gracefully handles fetch failures by retrying certain number of attempts" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream, max_load_attempts: 3)

      stub(FoldMock, :initial, fn -> :initial end)

      expect(StoreMock, :fetch_events, 2, fn ^stream, -1 -> raise RuntimeError end)
      expect(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)
      assert :initial = Decider.query(pid, & &1)
      refute_receive {:EXIT, ^pid, _}
    end

    test "respects max_load_attempts setting when retrying" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream, max_load_attempts: 2)

      stub(FoldMock, :initial, fn -> :initial end)

      expect(StoreMock, :fetch_events, 2, fn ^stream, -1 -> raise RuntimeError end)

      assert capture_exit(fn -> Decider.Stateful.start_link(decider) end) =~ "RuntimeError"
    end

    test "codec errors do not trigger retries and instead just crash the process" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream, max_load_attempts: 3)

      stub(FoldMock, :initial, fn -> :initial end)
      stub(StoreMock, :fetch_events, fn ^stream, -1 -> [build(:timeline_event)] end)

      expect(CodecMock, :decode, fn _ -> raise RuntimeError end)

      assert capture_exit(fn -> Decider.Stateful.start_link(decider) end) =~ "CodecError"
    end

    test "fold errors do not trigger retries and instead just crash the process" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream, max_load_attempts: 3)

      stub(FoldMock, :initial, fn -> :initial end)
      stub(CodecMock, :decode, &{:ok, &1.data})
      stub(StoreMock, :fetch_events, fn ^stream, -1 -> [build(:timeline_event)] end)

      expect(FoldMock, :evolve, fn _, _ -> raise RuntimeError end)

      assert capture_exit(fn -> Decider.Stateful.start_link(decider) end) =~ "FoldError"
    end
  end

  describe "query/2" do
    test "allows to query decider state via provided callback function" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> :some_value end)
      stub(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)
      assert :some_value = Decider.query(pid, & &1)
    end

    test "query callback errors are not captured and do crash the process" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> :initial end)
      stub(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)

      assert capture_exit(fn -> Decider.query(pid, fn _ -> raise RuntimeError end) end) =~
               "RuntimeError"
    end
  end

  describe "transact/3" do
    test "executes decision callback, writes events it produced and folds them back into state" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, :context -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})
      stub(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)

      expect(StoreMock, :write_events, fn ^stream, events, -1 ->
        assert [%EventData{data: 2}, %EventData{data: 3}] = events
        {:ok, 1}
      end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)
      assert {:ok, ^pid} = Decider.transact(pid, fn 0 -> [2, 3] end, :context)
      assert 5 = Decider.query(pid, & &1)
    end

    test "decision callback returning nil or empty list does nothing" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)
      assert {:ok, ^pid} = Decider.transact(pid, fn 0 -> nil end)
      assert {:ok, ^pid} = Decider.transact(pid, fn 0 -> [] end)
    end

    test "decision callback returning error simply propagates it back" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)
      assert {:error, :custom_error} = Decider.transact(pid, fn 0 -> {:error, :custom_error} end)
    end

    test "decision callback exceptions are not captured and do crash the process" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)

      assert capture_exit(fn -> Decider.transact(pid, fn _ -> raise RuntimeError end) end) =~
               "RuntimeError"
    end

    test "respects previous events" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})

      expect(StoreMock, :fetch_events, fn ^stream, -1 ->
        [build(:timeline_event, data: 2, position: 0)]
      end)

      expect(StoreMock, :write_events, fn ^stream, events, 0 ->
        assert [%EventData{data: 3}] = events
        {:ok, 1}
      end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)
      assert {:ok, ^pid} = Decider.transact(pid, fn 2 -> 3 end)
      assert 5 = Decider.query(pid, & &1)
    end

    test "gracefully handles expected version conflict by reloading the state and redoing the decision" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})

      expect(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)

      expect(StoreMock, :write_events, fn ^stream, _, -1 -> {:error, %StreamVersionConflict{}} end)

      expect(StoreMock, :fetch_events, fn ^stream, -1 ->
        [build(:timeline_event, data: 2, position: 0)]
      end)

      expect(StoreMock, :write_events, fn ^stream, events, 0 -> {:ok, length(events)} end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)
      assert {:ok, ^pid} = Decider.transact(pid, fn _ -> 3 end)
      assert 5 = Decider.query(pid, & &1)
    end

    test "respects max_resync_attempts setting when redoing the decision" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream, max_resync_attempts: 0)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})
      stub(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)

      expect(StoreMock, :write_events, fn ^stream, _, -1 -> {:error, %StreamVersionConflict{}} end)

      {:ok, pid} = Decider.Stateful.start_link(decider)

      assert capture_exit(fn -> Decider.transact(pid, & &1) end) =~ "StreamVersionConflict"
    end

    test "gracefully handles general event write errors by retrying certain number of attempts" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})

      expect(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)
      expect(StoreMock, :write_events, 2, fn ^stream, _, -1 -> raise RuntimeError end)
      expect(StoreMock, :write_events, fn ^stream, events, -1 -> {:ok, length(events)} end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)
      assert {:ok, ^pid} = Decider.transact(pid, fn _ -> 3 end)
      assert 3 = Decider.query(pid, & &1)
    end

    test "respects max_write_attempts setting when redoing the writes" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream, max_write_attempts: 1)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(FoldMock, :evolve, &(&1 + &2))
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})

      expect(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)
      expect(StoreMock, :write_events, fn ^stream, _, -1 -> raise RuntimeError end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)

      assert capture_exit(fn -> Decider.transact(pid, & &1) end) =~ "RuntimeError"
    end

    test "fold errors do not trigger retries" do
      stream = build(:stream_name)
      decider = build(:decider, stream_name: stream)

      stub(FoldMock, :initial, fn -> 0 end)
      stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
      stub(CodecMock, :decode, &{:ok, &1.data})

      stub(FoldMock, :evolve, fn _, _ -> raise RuntimeError end)
      expect(StoreMock, :fetch_events, fn ^stream, -1 -> [] end)
      expect(StoreMock, :write_events, fn ^stream, events, -1 -> {:ok, length(events)} end)

      assert {:ok, pid} = Decider.Stateful.start_link(decider)

      assert capture_exit(fn -> Decider.transact(pid, & &1) end) =~ "FoldError"
    end
  end

  defp capture_exit(fun) do
    capture_log(fn ->
      Process.flag(:trap_exit, true)

      try do
        fun.()
      catch
        :exit, _ -> nil
      end

      assert_receive {:EXIT, _, _}
    end)
  end

  defp build(fixture, attrs \\ [])

  defp build(:stream_name, _attrs) do
    StreamName.parse!("Invoice-1")
  end

  defp build(:decider, attrs) do
    test_pid = self()

    Decider.Stateful.for_stream(
      Keyword.get(attrs, :stream_name, build(:stream_name)),
      supervisor: :disabled,
      registry: :disabled,
      store: StoreMock,
      codec: CodecMock,
      fold: FoldMock,
      opts: [
        max_load_attempts: Keyword.get(attrs, :max_load_attempts, 3),
        max_write_attempts: Keyword.get(attrs, :max_write_attempts, 3),
        max_resync_attempts: Keyword.get(attrs, :max_resync_attempts, 1),
        on_init: fn ->
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
