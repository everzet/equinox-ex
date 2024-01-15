defmodule Equinox.Decider.CommonTest do
  use ExUnit.Case, async: true

  import Mox
  import ExUnit.CaptureLog

  alias Equinox.{Store, Decider}
  alias Equinox.Store.State
  alias Equinox.Decider.Decision
  alias Equinox.TestMocks.{StoreMock, LifetimeMock}

  setup :verify_on_exit!

  # We test all versions of decider with same test suite
  Enum.each([Decider, Decider.Async], fn decider_mod ->
    describe "#{inspect(decider_mod)} initialization" do
      @stream "Invoice-1"

      test "loads stream state using provided store, codec and fold modules" do
        expect(StoreMock, :load, fn @stream, nil, _ ->
          {:ok, State.new(7, 2)}
        end)

        decider = init(unquote(decider_mod), stream_name: @stream)
        assert {7, ^decider} = Decider.query(decider, & &1)
      end

      test "gracefully handles normal load errors (not exceptions) by retrying" do
        expect(StoreMock, :load, fn _, nil, _ ->
          {:error, %RuntimeError{}, State.new(:initial, -1)}
        end)

        expect(StoreMock, :load, fn _, _, _ -> {:ok, State.new(:loaded, -1)} end)

        decider = init(unquote(decider_mod), max_load_attempts: 2)

        assert {:loaded, ^decider} = Decider.query(decider, & &1)
      end

      test "respects partially loaded state when retrying" do
        expect(StoreMock, :load, fn _, nil, _ ->
          {:error, %RuntimeError{}, State.new(:partial, 0)}
        end)

        expect(StoreMock, :load, fn _, %{value: :partial, version: 0}, _ ->
          {:ok, State.new(:full, 1)}
        end)

        decider = init(unquote(decider_mod), max_load_attempts: 2)

        assert {:full, ^decider} = Decider.query(decider, & &1)
      end

      test "does not retry past max_load_attempts setting" do
        expect(StoreMock, :load, 2, fn _, _, _ ->
          {:error, %RuntimeError{message: "some error"}, State.new(:initial, -1)}
        end)

        assert capture_crash(fn -> init(unquote(decider_mod), max_load_attempts: 2) end) =~
                 "some error"
      end

      test "exceptions do not trigger load retries as they are assumed to be unrecoverable" do
        expect(StoreMock, :load, 1, fn _, nil, _ ->
          raise RuntimeError, message: "fold exception"
        end)

        assert capture_crash(fn -> init(unquote(decider_mod), max_load_attempts: 2) end) =~
                 "fold exception"
      end
    end

    describe "#{inspect(decider_mod)}.query/2" do
      test "executes query callback with loaded state value and returns whatever it returns" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(:value, -1)} end)

        decider = init(unquote(decider_mod))

        assert {:value, ^decider} = Decider.query(decider, & &1)
      end

      test "query callback exceptions are not caught" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(:initial, -1)} end)

        decider = init(unquote(decider_mod))

        assert capture_crash(fn -> Decider.query(decider, fn _state -> raise RuntimeError end) end) =~
                 "RuntimeError"
      end
    end

    describe "#{inspect(decider_mod)}.transact/3" do
      @stream "Invoice-1"

      test "executes decision callback and syncs the resulting outcome using provided store, codec and fold" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, fn @stream, %{version: -1}, %{events: [2, 3]}, _ ->
          {:ok, State.new(5, 1)}
        end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert {:ok, decider} = Decider.transact(decider, fn 0 -> [2, 3] end)
        assert {5, ^decider} = Decider.query(decider, & &1)
      end

      test "passes optional sync context all the way to sync" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, fn _, %{version: -1}, %{context: %{value: 2}}, _ ->
          {:ok, State.new(5, 1)}
        end)

        decider = init(unquote(decider_mod))

        assert {:ok, _} = Decider.transact(decider, fn 0 -> [2] end, %{value: 2})
      end

      test "keeps track of state (stream) version during the sync process" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(2, 0)} end)

        expect(StoreMock, :sync, fn _, %{version: 0}, %{events: [3]}, _ ->
          {:ok, State.new(5, 1)}
        end)

        decider = init(unquote(decider_mod))

        assert {:ok, decider} = Decider.transact(decider, fn 2 -> 3 end)
        assert {5, ^decider} = Decider.query(decider, & &1)
      end

      test "handles state<->stream version conflicts by reloading the state and redoing the decision" do
        # Expectation:

        # 1. Initial load has no events, so state value is `0` and state version is -1
        expect(StoreMock, :load, fn _, nil, _ -> {:ok, State.new(0, -1)} end)
        decider = init(unquote(decider_mod), max_resync_attempts: 1)

        # 2. In the meantime, someone else writes event `2` to the stream, stream version is now 0

        # 3. We make the `+ 3` decision based on state value 0 (version -1), producing `0 + 3 = 3` event
        decision = &(&1 + 3)

        # 4. We fail to sync the result of the decision due to the version conflict (-1 != 0)
        expect(StoreMock, :sync, fn _, %{version: -1}, %{events: [3]}, _ ->
          {:error, %Store.StreamVersionConflict{}}
        end)

        # 5. We automatically reload the stream and arrive at the new state value `2`, version 0
        expect(StoreMock, :load, fn _, %{version: -1}, _ -> {:ok, State.new(2, 0)} end)

        # 6. We redo the `+ 3` decision based on the new state value `2`, producing `2 + 3 = 5` event

        # 7. We successfully sync the result of the updated decision to the stream and produce new state
        expect(StoreMock, :sync, fn _, %{version: 0}, %{events: [5]}, _ ->
          {:ok, State.new(7, 1)}
        end)

        # Execution:

        assert {:ok, decider} = Decider.transact(decider, decision)
        # expect final state to be addition of both events: `2 + 5 = 7`
        assert {7, ^decider} = Decider.query(decider, & &1)
      end

      test "does not try to resync the decision past max_resync_attempts setting" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 1, fn _, %{version: -1}, _, _ ->
          {:error, %Store.StreamVersionConflict{}}
        end)

        decider = init(unquote(decider_mod), max_resync_attempts: 0)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "StreamVersionConflict"
      end

      test "does retry failing loads during resync up to configured max_load_attempts" do
        expect(StoreMock, :load, fn _, nil, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, fn _, %{version: -1}, _, _ ->
          {:error, %Store.StreamVersionConflict{}}
        end)

        expect(StoreMock, :load, 2, fn _, %{version: -1} = state, _ ->
          {:error, %RuntimeError{}, state}
        end)

        decider = init(unquote(decider_mod), max_resync_attempts: 1, max_load_attempts: 2)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "RuntimeError"
      end

      test "handles general sync errors (not conflicts or exceptions) by retrying" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, fn _, %{version: -1}, _, _ -> {:error, %RuntimeError{}} end)
        expect(StoreMock, :sync, fn _, %{version: -1}, _, _ -> {:ok, State.new(3, 1)} end)

        decider = init(unquote(decider_mod), max_sync_attempts: 2)

        assert {:ok, decider} = Decider.transact(decider, fn _ -> 3 end)
        assert {3, ^decider} = Decider.query(decider, & &1)
      end

      test "does not retry sync past max_sync_attempts setting" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 1, fn _, %{version: -1}, _, _ -> {:error, %RuntimeError{}} end)

        decider = init(unquote(decider_mod), max_sync_attempts: 1)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "RuntimeError"
      end

      test "exceptions do not trigger sync retries as they are assumed to be unrecoverable" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 1, fn _, %{version: -1}, _, _ -> raise RuntimeError end)

        decider = init(unquote(decider_mod), max_sync_attempts: 2)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "RuntimeError"
      end

      test "decision callbacks returning result and events propagate result after sync" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, fn _, _, %{events: [1]}, _ -> {:ok, State.new(1, 0)} end)
        expect(StoreMock, :sync, fn _, _, %{events: [2]}, _ -> {:ok, State.new(2, 1)} end)

        decider = init(unquote(decider_mod))

        assert {:ok, :one, decider} = Decider.transact(decider, fn 0 -> {:ok, :one, [1]} end)
        assert {:ok, :two, _decider} = Decider.transact(decider, fn 1 -> {:two, [2]} end)
      end

      test "decision callbacks returning nil or empty list do not trigger sync" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 0, fn _, _, _, _ -> raise RuntimeError end)

        decider = init(unquote(decider_mod))

        assert {:ok, ^decider} = Decider.transact(decider, fn 0 -> nil end)
        assert {:ok, ^decider} = Decider.transact(decider, fn 0 -> [] end)
      end

      test "decision callback errors are propagated back without triggering sync" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 0, fn _, _, _, _ -> raise RuntimeError end)

        decider = init(unquote(decider_mod))

        assert {:error, %Decision.Error{term: :custom_error}, _} =
                 Decider.transact(decider, fn 0 -> {:error, :custom_error} end)
      end

      test "decision callback exceptions are not caught" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 0, fn _, _, _, _ -> raise ArgumentError end)

        decider = init(unquote(decider_mod))

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

  defp init(Decider, attrs) do
    attrs
    |> Keyword.get(:stream_name, "Invoice-1")
    |> Decider.load(
      store: {StoreMock, allow_mocks_from: self()},
      max_load_attempts: Keyword.get(attrs, :max_load_attempts, 1),
      max_sync_attempts: Keyword.get(attrs, :max_sync_attempts, 1),
      max_resync_attempts: Keyword.get(attrs, :max_resync_attempts, 0)
    )
  end

  defp init(Decider.Async, attrs) do
    stub(LifetimeMock, :after_init, fn _ -> :timer.seconds(10) end)
    stub(LifetimeMock, :after_query, fn _ -> :timer.seconds(10) end)
    stub(LifetimeMock, :after_transact, fn _ -> :timer.seconds(10) end)

    init(Decider, attrs)
    |> Decider.start(
      supervisor: :disabled,
      registry: :disabled,
      lifetime: LifetimeMock
    )
  end
end
