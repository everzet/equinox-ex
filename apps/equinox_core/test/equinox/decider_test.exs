defmodule Equinox.DeciderTest do
  use ExUnit.Case, async: true

  import Mox
  import ExUnit.CaptureLog

  alias Equinox.Stream.StreamName
  alias Equinox.{State, Store, Codec, Fold, Decider, Lifetime}
  alias Equinox.TestMocks.{StoreMock, CodecMock, FoldMock}

  setup :verify_on_exit!

  # We test both Stateful and Stateless versions of decider with same test suite
  Enum.each([Decider.Stateful, Decider.Stateless], fn decider_mod ->
    describe "#{inspect(decider_mod)} initialization" do
      @stream "Invoice-1"

      test "loads stream state" do
        stub(FoldMock, :initial, fn -> 1 end)

        expect(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(7, 2) end)

        decider = init(unquote(decider_mod), stream_name: StreamName.parse!(@stream))
        assert 7 = Decider.query(decider, & &1)
      end

      test "gracefully handles load exceptions by retrying" do
        stub(FoldMock, :initial, fn -> :initial end)

        expect(StoreMock, :load!, fn @stream, _, _, _ -> raise RuntimeError end)
        expect(StoreMock, :load!, fn @stream, _, _, _ -> State.new(:initial, -1) end)

        decider =
          init(unquote(decider_mod),
            stream_name: StreamName.parse!(@stream),
            max_load_attempts: 2
          )

        assert :initial = Decider.query(decider, & &1)
      end

      test "does not retry past max_load_attempts option" do
        stub(FoldMock, :initial, fn -> :initial end)

        expect(StoreMock, :load!, 2, fn @stream, %{version: -1}, _, _ -> raise RuntimeError end)

        assert capture_crash(fn ->
                 init(unquote(decider_mod),
                   stream_name: StreamName.parse!(@stream),
                   max_load_attempts: 2
                 )
               end) =~ "RuntimeError"
      end

      test "codec errors never trigger load retries as they should be unrecoverable" do
        stub(FoldMock, :initial, fn -> :initial end)

        expect(StoreMock, :load!, 1, fn @stream, %{version: -1}, _, _ ->
          raise Codec.CodecError, message: "codec error"
        end)

        assert capture_crash(fn ->
                 init(unquote(decider_mod),
                   stream_name: StreamName.parse!(@stream),
                   max_load_attempts: 2
                 )
               end) =~ "CodecError"
      end

      test "fold exceptions never trigger fetch retries as they should be unrecoberable" do
        stub(FoldMock, :initial, fn -> :initial end)

        expect(StoreMock, :load!, 1, fn @stream, %{version: -1}, _, _ ->
          raise Fold.FoldError, message: "fold error"
        end)

        assert capture_crash(fn ->
                 init(unquote(decider_mod),
                   stream_name: StreamName.parse!(@stream),
                   max_load_attempts: 2
                 )
               end) =~ "FoldError"
      end
    end

    describe "#{inspect(decider_mod)}.query/2" do
      test "executes query callback with current decider state and returns whatever it returns" do
        stub(FoldMock, :initial, fn -> :value end)
        stub(StoreMock, :load!, fn _, %{version: -1}, _, _ -> State.new(:value, -1) end)

        decider = init(unquote(decider_mod))

        assert :value = Decider.query(decider, & &1)
      end

      test "query callback exceptions are not captured" do
        stub(FoldMock, :initial, fn -> :initial end)
        stub(StoreMock, :load!, fn _, %{version: -1}, _, _ -> State.new(:initial, -1) end)

        decider = init(unquote(decider_mod))

        assert capture_crash(fn -> Decider.query(decider, fn _state -> raise RuntimeError end) end) =~
                 "RuntimeError"
      end
    end

    describe "#{inspect(decider_mod)}.transact/3" do
      @stream "Invoice-1"

      test "executes decision callback and syncs the resulting events" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, fn @stream, %{version: -1}, [2, 3], :ctx, CodecMock, FoldMock ->
          State.new(5, 1)
        end)

        decider = init(unquote(decider_mod), stream_name: StreamName.parse!(@stream))

        assert {:ok, decider} = Decider.transact(decider, fn 0 -> [2, 3] end, :ctx)
        assert 5 = Decider.query(decider, & &1)
      end

      test "fully incorporates previously written events into the process" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(2, 0) end)

        expect(StoreMock, :sync!, fn @stream, %{version: 0}, [3], _, CodecMock, FoldMock ->
          State.new(5, 1)
        end)

        decider = init(unquote(decider_mod), stream_name: StreamName.parse!(@stream))

        assert {:ok, decider} = Decider.transact(decider, fn 2 -> 3 end)
        assert 5 = Decider.query(decider, & &1)
      end

      test "gracefully handles version conflicts by resyncing the state and redoing the decision" do
        stub(FoldMock, :initial, fn -> 0 end)

        # Expectation:

        # 1. Initial load has no events, so state is `0` and its version is -1
        expect(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        decider =
          init(unquote(decider_mod),
            stream_name: StreamName.parse!(@stream),
            max_resync_attempts: 1
          )

        # 2. In the meantime, someone else writes event `2` to the stream, its version is now 0

        # 3. We make the `+ 3` decision based on state `0`, producing `0 + 3 = 3` event

        # 4. We fail to sync the result of the decision due to stream version conflict
        expect(StoreMock, :sync!, fn @stream, %{version: -1}, [3], :ctx, CodecMock, FoldMock ->
          raise Store.StreamVersionConflict
        end)

        # 5. We automatically resync the stream and arrive at the new state `2` of version 0
        expect(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(2, 0) end)

        # 6. We redo the `+ 3` decision based on the new state `2`, producing `2 + 3 = 5` event

        # 7. We successfully sync the result of the updated decision to the stream and produce new state
        expect(StoreMock, :sync!, fn @stream, %{version: 0}, [5], :ctx, CodecMock, FoldMock ->
          State.new(7, 1)
        end)

        # 8. Final state should be sum of both events: `2 + 5 = 7`

        # Execution:

        # produce an event that is current state + 3
        assert {:ok, decider} = Decider.transact(decider, fn state -> state + 3 end, :ctx)

        # expect final state to be addition of both events: `2 + 5 = 7`
        assert 7 = Decider.query(decider, & &1)
      end

      test "does not try to resync past max_resync_attempts option" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          raise Store.StreamVersionConflict
        end)

        decider =
          init(unquote(decider_mod),
            stream_name: StreamName.parse!(@stream),
            max_resync_attempts: 0
          )

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "StreamVersionConflict"
      end

      test "gracefully handles sync errors (non-conflicts) by retrying" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          raise RuntimeError
        end)

        expect(StoreMock, :sync!, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          State.new(3, 1)
        end)

        decider =
          init(unquote(decider_mod),
            stream_name: StreamName.parse!(@stream),
            max_sync_attempts: 2
          )

        assert {:ok, decider} = Decider.transact(decider, fn _ -> 3 end)
        assert 3 = Decider.query(decider, & &1)
      end

      test "does not retry sync past max_sync_attempts option" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          raise RuntimeError
        end)

        decider =
          init(unquote(decider_mod),
            stream_name: StreamName.parse!(@stream),
            max_sync_attempts: 1
          )

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "RuntimeError"
      end

      test "codec exceptions never trigger write retries as they should be unrecoverable" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          raise Codec.CodecError, message: "codec error"
        end)

        decider =
          init(unquote(decider_mod),
            stream_name: StreamName.parse!(@stream),
            max_sync_attempts: 2
          )

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "CodecError"
      end

      test "fold exceptions never trigger write retries as they should be unrecoverable" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          raise Fold.FoldError, message: "fold error"
        end)

        decider =
          init(unquote(decider_mod),
            stream_name: StreamName.parse!(@stream),
            max_sync_attempts: 2
          )

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "FoldError"
      end

      test "decision callbacks returning nil or empty list do not trigger sync" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, 0, fn @stream, _, _, _, CodecMock, FoldMock -> nil end)

        decider = init(unquote(decider_mod), stream_name: StreamName.parse!(@stream))

        assert {:ok, decider} = Decider.transact(decider, fn 0 -> nil end)
        assert {:ok, _decider} = Decider.transact(decider, fn 0 -> [] end)
      end

      test "decision callback errors are propagated back without triggering sync" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, 0, fn @stream, _, _, _, CodecMock, FoldMock -> nil end)

        decider = init(unquote(decider_mod), stream_name: StreamName.parse!(@stream))

        assert {:error, :custom_error} =
                 Decider.transact(decider, fn 0 -> {:error, :custom_error} end)
      end

      test "decision callback exceptions are not captured" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, 0, fn @stream, _, _, _, CodecMock, FoldMock -> nil end)

        decider = init(unquote(decider_mod), stream_name: StreamName.parse!(@stream))

        assert capture_crash(fn -> Decider.transact(decider, fn _ -> raise RuntimeError end) end) =~
                 "RuntimeError"
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
    Decider.Stateless.for_stream(
      Keyword.get(attrs, :stream_name, StreamName.parse!("Invoice-1")),
      store: StoreMock,
      codec: CodecMock,
      fold: FoldMock,
      max_load_attempts: Keyword.get(attrs, :max_load_attempts, 2),
      max_sync_attempts: Keyword.get(attrs, :max_sync_attempts, 2),
      max_resync_attempts: Keyword.get(attrs, :max_resync_attempts, 1)
    )
    |> Decider.load()
  end

  defp init(Decider.Stateful, attrs) do
    test_pid = self()

    Decider.Stateful.for_stream(
      Keyword.get(attrs, :stream_name, StreamName.parse!("Invoice-1")),
      supervisor: :disabled,
      registry: :disabled,
      lifetime: Lifetime.StayAliveFor30Seconds,
      store: StoreMock,
      codec: CodecMock,
      fold: FoldMock,
      max_load_attempts: Keyword.get(attrs, :max_load_attempts, 2),
      max_sync_attempts: Keyword.get(attrs, :max_sync_attempts, 2),
      max_resync_attempts: Keyword.get(attrs, :max_resync_attempts, 1),
      on_init: fn ->
        allow(StoreMock, test_pid, self())
        allow(FoldMock, test_pid, self())
      end
    )
    |> Decider.load()
  end
end
