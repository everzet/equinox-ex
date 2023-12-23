defmodule Equinox.DeciderTest do
  use ExUnit.Case, async: true

  import Mox
  import ExUnit.CaptureLog

  alias Equinox.TestMocks.{StoreMock, CodecMock, FoldMock}
  alias Equinox.Events.{TimelineEvent, EventData}
  alias Equinox.Store.StreamVersionConflict
  alias Equinox.Stream.StreamName
  alias Equinox.{Decider, Lifetime, UUID}

  setup :verify_on_exit!

  # We test both Stateful and Stateless versions of decider with same test suite
  Enum.each([Decider.Stateful, Decider.Stateless], fn decider_mod ->
    describe "#{inspect(decider_mod)} initialization" do
      @stream StreamName.parse!("Invoice-1")

      test "sets state to initial if stream is empty" do
        stub(FoldMock, :initial, fn -> :initial end)
        stub(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [] end)

        decider = init(unquote(decider_mod), stream_name: @stream)
        assert :initial = Decider.query(decider, & &1)
      end

      test "folds events into initial state if stream has some" do
        stub(CodecMock, :decode, &{:ok, &1.data})
        stub(FoldMock, :initial, fn -> 1 end)
        stub(FoldMock, :evolve, &(&1 + &2))

        expect(StoreMock, :fetch_timeline_events, fn @stream, -1 ->
          [
            build(:timeline_event, data: 1),
            build(:timeline_event, data: 2),
            build(:timeline_event, data: 3)
          ]
        end)

        decider = init(unquote(decider_mod), stream_name: @stream)
        assert 7 = Decider.query(decider, & &1)
      end

      test "gracefully handles event fetch failures by retrying" do
        stub(FoldMock, :initial, fn -> :initial end)

        expect(StoreMock, :fetch_timeline_events, 2, fn @stream, -1 -> raise RuntimeError end)
        expect(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [] end)

        decider = init(unquote(decider_mod), stream_name: @stream)
        assert :initial = Decider.query(decider, & &1)
      end

      test "does not retry past max_load_attempts option" do
        stub(FoldMock, :initial, fn -> :initial end)

        expect(StoreMock, :fetch_timeline_events, 2, fn @stream, -1 -> raise RuntimeError end)

        assert capture_crash(fn ->
                 init(unquote(decider_mod), stream_name: @stream, max_load_attempts: 2)
               end) =~ "RuntimeError"
      end

      test "codec errors never trigger fetch retries as they should be unrecoverable" do
        stub(FoldMock, :initial, fn -> :initial end)
        stub(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [build(:timeline_event)] end)

        expect(CodecMock, :decode, fn _ -> raise RuntimeError end)

        assert capture_crash(fn -> init(unquote(decider_mod), stream_name: @stream) end) =~
                 "RuntimeError"
      end

      test "fold errors never trigger fetch retries as they should be unrecoberable" do
        stub(FoldMock, :initial, fn -> :initial end)
        stub(CodecMock, :decode, &{:ok, &1.data})
        stub(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [build(:timeline_event)] end)

        expect(FoldMock, :evolve, fn _, _ -> raise RuntimeError end)

        assert capture_crash(fn -> init(unquote(decider_mod), stream_name: @stream) end) =~
                 "FoldError"
      end
    end

    describe "#{inspect(decider_mod)}.query/2" do
      test "executes query callback with current decider state and returns whatever it returns" do
        stub(FoldMock, :initial, fn -> :some_value end)
        stub(StoreMock, :fetch_timeline_events, fn _stream, _pos -> [] end)

        decider = init(unquote(decider_mod))

        assert :some_value = Decider.query(decider, & &1)
      end

      test "query callback exceptions are reraised as QueryError" do
        stub(FoldMock, :initial, fn -> :initial end)
        stub(StoreMock, :fetch_timeline_events, fn _stream, _pos -> [] end)

        decider = init(unquote(decider_mod))

        assert capture_crash(fn -> Decider.query(decider, fn _state -> raise RuntimeError end) end) =~
                 "QueryError"
      end
    end

    describe "#{inspect(decider_mod)}.transact/3" do
      @stream StreamName.parse!("Invoice-1")

      test "executes decision callback, writes events it produces and folds them back into the state" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(FoldMock, :evolve, &(&1 + &2))
        stub(CodecMock, :encode, fn e, :context -> {:ok, build(:event_data, data: e)} end)
        stub(CodecMock, :decode, &{:ok, &1.data})
        stub(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [] end)

        expect(StoreMock, :write_event_data, fn @stream, events, -1 ->
          assert [%EventData{data: 2}, %EventData{data: 3}] = events
          {:ok, 1}
        end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert {:ok, decider} = Decider.transact(decider, fn 0 = _state -> [2, 3] end, :context)
        assert 5 = Decider.query(decider, & &1)
      end

      test "fully incorporates previously written events into the process" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(FoldMock, :evolve, &(&1 + &2))
        stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
        stub(CodecMock, :decode, &{:ok, &1.data})

        expect(StoreMock, :fetch_timeline_events, fn @stream, -1 ->
          [build(:timeline_event, data: 2, position: 0)]
        end)

        expect(StoreMock, :write_event_data, fn @stream, [%{data: 3}], 0 ->
          {:ok, 1}
        end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert {:ok, decider} = Decider.transact(decider, fn 2 = _state -> 3 end)
        assert 5 = Decider.query(decider, & &1)
      end

      test "gracefully handles version conflict by resyncing the state and redoing the decision" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(FoldMock, :evolve, &(&1 + &2))
        stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
        stub(CodecMock, :decode, &{:ok, &1.data})

        # no events on initial load, so state is `0`
        expect(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [] end)
        decider = init(unquote(decider_mod), stream_name: @stream)

        # failing to write result of `0 + 3 = 3` decision due to stream version conflict
        expect(StoreMock, :write_event_data, fn @stream, [%{data: 3}], -1 ->
          {:error, %StreamVersionConflict{}}
        end)

        # fetch new event `2` from the stream and fold it into new `0 + 2 = 2` state
        expect(StoreMock, :fetch_timeline_events, fn @stream, -1 ->
          [build(:timeline_event, data: 2, position: 0)]
        end)

        # successfully write the result of redone `2 + 3 = 5` decision
        expect(StoreMock, :write_event_data, fn @stream, [%{data: 5}], 0 ->
          {:ok, 1}
        end)

        assert {:ok, decider} = Decider.transact(decider, fn state -> state + 3 end)

        # state is a simple addition of all event values, so `2 + 5 = 7`
        assert 7 = Decider.query(decider, & &1)
      end

      test "does not try to resync past max_resync_attempts option" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(FoldMock, :evolve, &(&1 + &2))
        stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
        stub(CodecMock, :decode, &{:ok, &1.data})

        expect(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [] end)

        expect(StoreMock, :write_event_data, fn @stream, _, -1 ->
          {:error, %StreamVersionConflict{}}
        end)

        decider = init(unquote(decider_mod), stream_name: @stream, max_resync_attempts: 0)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "StreamVersionConflict"
      end

      test "gracefully handles other event write failures by retrying" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(FoldMock, :evolve, &(&1 + &2))
        stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
        stub(CodecMock, :decode, &{:ok, &1.data})

        expect(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [] end)
        expect(StoreMock, :write_event_data, 2, fn @stream, _, -1 -> raise RuntimeError end)
        expect(StoreMock, :write_event_data, fn @stream, events, -1 -> {:ok, length(events)} end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert {:ok, decider} = Decider.transact(decider, fn _ -> 3 end)
        assert 3 = Decider.query(decider, & &1)
      end

      test "does not retry writes past max_write_attempts option" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(FoldMock, :evolve, &(&1 + &2))
        stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
        stub(CodecMock, :decode, &{:ok, &1.data})

        expect(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [] end)
        expect(StoreMock, :write_event_data, fn @stream, _, -1 -> raise RuntimeError end)

        decider = init(unquote(decider_mod), stream_name: @stream, max_write_attempts: 1)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "RuntimeError"
      end

      test "fold errors never trigger write retries as they should be unrecoverable" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(CodecMock, :encode, fn e, _ -> {:ok, build(:event_data, data: e)} end)
        stub(CodecMock, :decode, &{:ok, &1.data})

        stub(FoldMock, :evolve, fn _, _ -> raise RuntimeError end)
        expect(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [] end)
        expect(StoreMock, :write_event_data, fn @stream, events, -1 -> {:ok, length(events)} end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "FoldError"
      end

      test "decision callbacks returning nil or empty list do not trigger sync" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [] end)

        expect(StoreMock, :write_event_data, 0, fn @stream, _, _ -> {:ok, 0} end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert {:ok, decider} = Decider.transact(decider, fn 0 -> nil end)
        assert {:ok, _decider} = Decider.transact(decider, fn 0 -> [] end)
      end

      test "decision callback errors are propagated back without triggering sync" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [] end)

        expect(StoreMock, :write_event_data, 0, fn @stream, _, _ -> {:ok, 0} end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert {:error, :custom_error} =
                 Decider.transact(decider, fn 0 -> {:error, :custom_error} end)
      end

      test "decision callback exceptions are reraised as DecisionError" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :fetch_timeline_events, fn @stream, -1 -> [] end)

        expect(StoreMock, :write_event_data, 0, fn @stream, _, _ -> {:ok, 0} end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert capture_crash(fn -> Decider.transact(decider, fn _ -> raise RuntimeError end) end) =~
                 "DecisionError"
      end
    end
  end)

  defp capture_crash(fun) do
    capture_log(fn ->
      Process.flag(:trap_exit, true)
      require Logger

      try do
        fun.()

        receive do
          {:EXIT, reason, term} -> Logger.error(inspect(reason: reason, term: term))
        after
          10 -> nil
        end
      rescue
        exception -> Logger.error(inspect(exception))
      catch
        :exit, exit -> Logger.error(inspect(exit))
      end
    end)
  end

  defp init(decider_mod, attrs \\ [])

  defp init(Decider.Stateless, attrs) do
    attrs
    |> Keyword.get(:stream_name, StreamName.parse!("Invoice-1"))
    |> Decider.Stateless.for_stream(
      store: StoreMock,
      codec: CodecMock,
      fold: FoldMock,
      opts: [
        max_load_attempts: Keyword.get(attrs, :max_load_attempts, 3),
        max_write_attempts: Keyword.get(attrs, :max_write_attempts, 3),
        max_resync_attempts: Keyword.get(attrs, :max_resync_attempts, 1)
      ]
    )
    |> Decider.Stateless.load()
  end

  defp init(Decider.Stateful, attrs) do
    test_pid = self()

    attrs
    |> Keyword.get(:stream_name, StreamName.parse!("Invoice-1"))
    |> Decider.Stateful.for_stream(
      supervisor: :disabled,
      registry: :disabled,
      lifetime: Lifetime.StayAliveFor30Seconds,
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
    |> Decider.Stateful.start_server()
    |> then(&elem(&1, 1))
  end

  defp build(type, attrs \\ [])

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
