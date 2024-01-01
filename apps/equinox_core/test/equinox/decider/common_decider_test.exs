defmodule Equinox.CommonDeciderTest do
  use ExUnit.Case, async: true

  import Mox
  import ExUnit.CaptureLog

  alias Equinox.{State, Store, Codec, Fold, Decider}
  alias Equinox.TestMocks.{StoreMock, CodecMock, FoldMock, LifetimeMock}

  setup :verify_on_exit!

  # We test both Stateful and Stateless versions of decider with same test suite
  Enum.each([Decider.Stateful, Decider.Stateless], fn decider_mod ->
    describe "#{inspect(decider_mod)} initialization" do
      @stream "Invoice-1"

      test "loads stream state" do
        expect(FoldMock, :initial, fn -> 1 end)
        expect(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(7, 2) end)

        decider = init(unquote(decider_mod), stream_name: @stream)
        assert Decider.query(decider, & &1) == 7
      end

      test "gracefully handles load exceptions by retrying" do
        stub(FoldMock, :initial, fn -> :initial end)

        expect(StoreMock, :load!, fn @stream, _, _, _ -> raise RuntimeError end)
        expect(StoreMock, :load!, fn @stream, _, _, _ -> State.new(:initial, -1) end)

        decider = init(unquote(decider_mod), stream_name: @stream, max_load_attempts: 2)

        assert Decider.query(decider, & &1) == :initial
      end

      test "does not retry past max_load_attempts setting" do
        stub(FoldMock, :initial, fn -> :initial end)

        expect(StoreMock, :load!, 2, fn @stream, %{version: -1}, _, _ -> raise RuntimeError end)

        assert capture_crash(fn ->
                 init(unquote(decider_mod), stream_name: @stream, max_load_attempts: 2)
               end) =~ "RuntimeError"
      end

      test "codec errors never trigger load retries as they should be unrecoverable" do
        stub(FoldMock, :initial, fn -> :initial end)

        expect(StoreMock, :load!, 1, fn @stream, %{version: -1}, _, _ ->
          raise Codec.CodecError, message: "codec error"
        end)

        assert capture_crash(fn ->
                 init(unquote(decider_mod), stream_name: @stream, max_load_attempts: 2)
               end) =~ "CodecError"
      end

      test "fold exceptions never trigger load retries as they should be unrecoberable" do
        stub(FoldMock, :initial, fn -> :initial end)

        expect(StoreMock, :load!, 1, fn @stream, %{version: -1}, _, _ ->
          raise Fold.FoldError, message: "fold error"
        end)

        assert capture_crash(fn ->
                 init(unquote(decider_mod), stream_name: @stream, max_load_attempts: 2)
               end) =~ "FoldError"
      end
    end

    describe "#{inspect(decider_mod)}.query/2" do
      test "executes query callback with loaded state value and returns whatever it returns" do
        stub(FoldMock, :initial, fn -> :value end)
        stub(StoreMock, :load!, fn _, %{version: -1}, _, _ -> State.new(:value, -1) end)

        decider = init(unquote(decider_mod))

        assert Decider.query(decider, & &1) == :value
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

      test "executes decision callback and syncs the resulting events into a new state" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, fn @stream,
                                     %{version: -1},
                                     [2, 3],
                                     %{c: 1},
                                     CodecMock,
                                     FoldMock ->
          State.new(5, 1)
        end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert {:ok, decider} = Decider.transact(decider, fn 0 -> [2, 3] end, %{c: 1})
        assert Decider.query(decider, & &1) == 5
      end

      test "keeps track of state (stream) version during the sync process" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(2, 0) end)

        expect(StoreMock, :sync!, fn @stream, %{version: 0}, [3], _, CodecMock, FoldMock ->
          State.new(5, 1)
        end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert {:ok, decider} = Decider.transact(decider, fn 2 -> 3 end)
        assert Decider.query(decider, & &1) == 5
      end

      test "handles state<->stream version conflicts by reloading the state and redoing the decision" do
        stub(FoldMock, :initial, fn -> 0 end)

        # Expectation:

        # 1. Initial load has no events, so state value is `0` and state version is -1
        expect(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)
        decider = init(unquote(decider_mod), stream_name: @stream, max_resync_attempts: 1)

        # 2. In the meantime, someone else writes event `2` to the stream, stream version is now 0

        # 3. We make the `+ 3` decision based on state value 0 (version -1), producing `0 + 3 = 3` event
        decision = &(&1 + 3)

        # 4. We fail to sync the result of the decision due to the version conflict (-1 != 0)
        expect(StoreMock, :sync!, fn @stream, %{version: -1}, [3], %{c: 1}, CodecMock, FoldMock ->
          raise Store.StreamVersionConflict
        end)

        # 5. We automatically reload the stream and arrive at the new state value `2`, version 0
        expect(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(2, 0) end)

        # 6. We redo the `+ 3` decision based on the new state value `2`, producing `2 + 3 = 5` event

        # 7. We successfully sync the result of the updated decision to the stream and produce new state
        expect(StoreMock, :sync!, fn @stream, %{version: 0}, [5], %{c: 1}, CodecMock, FoldMock ->
          State.new(7, 1)
        end)

        # Execution:

        assert {:ok, decider} = Decider.transact(decider, decision, %{c: 1})
        # expect final state to be addition of both events: `2 + 5 = 7`
        assert Decider.query(decider, & &1) == 7
      end

      test "does not try to resync the decision past max_resync_attempts setting" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, 1, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          raise Store.StreamVersionConflict
        end)

        decider = init(unquote(decider_mod), stream_name: @stream, max_resync_attempts: 0)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "StreamVersionConflict"
      end

      test "does not retry failing loads during resync (to avoid confusing overlap of settings)" do
        stub(FoldMock, :initial, fn -> 0 end)

        expect(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, fn @stream, %{version: -1}, _, _, _, _ ->
          raise Store.StreamVersionConflict
        end)

        expect(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> raise RuntimeError end)

        decider =
          init(unquote(decider_mod),
            stream_name: @stream,
            max_resync_attempts: 1,
            max_load_attempts: 2
          )

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "RuntimeError"
      end

      test "handles general sync errors (non-conflicts) by retrying" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          raise RuntimeError
        end)

        expect(StoreMock, :sync!, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          State.new(3, 1)
        end)

        decider = init(unquote(decider_mod), stream_name: @stream, max_sync_attempts: 2)

        assert {:ok, decider} = Decider.transact(decider, fn _ -> 3 end)
        assert Decider.query(decider, & &1) == 3
      end

      test "does not retry sync past max_sync_attempts setting" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, 1, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          raise RuntimeError
        end)

        decider = init(unquote(decider_mod), stream_name: @stream, max_sync_attempts: 1)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "RuntimeError"
      end

      test "codec errors and exceptions never trigger sync retries as they should be unrecoverable" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, 1, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          raise Codec.CodecError, message: "codec error"
        end)

        decider = init(unquote(decider_mod), stream_name: @stream, max_sync_attempts: 2)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "CodecError"
      end

      test "fold exceptions never trigger sync retries as they should be unrecoverable" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, 1, fn @stream, %{version: -1}, _, _, CodecMock, FoldMock ->
          raise Fold.FoldError, message: "fold error"
        end)

        decider = init(unquote(decider_mod), stream_name: @stream, max_sync_attempts: 2)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "FoldError"
      end

      test "decision callbacks returning nil or empty list do not trigger sync" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, 0, fn @stream, _, _, _, _, _ -> nil end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert {:ok, ^decider} = Decider.transact(decider, fn 0 -> nil end)
        assert {:ok, ^decider} = Decider.transact(decider, fn 0 -> [] end)
      end

      test "decision callback errors are propagated back without triggering sync" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, 0, fn @stream, _, _, _, _, _ -> nil end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert {:error, :custom_error} =
                 Decider.transact(decider, fn 0 -> {:error, :custom_error} end)
      end

      test "decision callback exceptions are not captured" do
        stub(FoldMock, :initial, fn -> 0 end)
        stub(StoreMock, :load!, fn @stream, %{version: -1}, _, _ -> State.new(0, -1) end)

        expect(StoreMock, :sync!, 0, fn @stream, _, _, _, _, _ -> nil end)

        decider = init(unquote(decider_mod), stream_name: @stream)

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
    attrs
    |> Keyword.get(:stream_name, "Invoice-1")
    |> Decider.load(
      store: StoreMock,
      codec: CodecMock,
      fold: FoldMock,
      context: %{allow_mocks_from: self()},
      max_load_attempts: Keyword.get(attrs, :max_load_attempts, 1),
      max_sync_attempts: Keyword.get(attrs, :max_sync_attempts, 1),
      max_resync_attempts: Keyword.get(attrs, :max_resync_attempts, 0)
    )
  end

  defp init(Decider.Stateful, attrs) do
    stub(LifetimeMock, :after_init, fn _ -> :timer.seconds(10) end)
    stub(LifetimeMock, :after_query, fn _ -> :timer.seconds(10) end)
    stub(LifetimeMock, :after_transact, fn _ -> :timer.seconds(10) end)

    init(Decider.Stateless, attrs)
    |> Decider.start(
      supervisor: :disabled,
      registry: :disabled,
      lifetime: LifetimeMock
    )
  end
end
