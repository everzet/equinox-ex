defmodule Equinox.Decider.CommonTest do
  use ExUnit.Case, async: true

  import Mox
  import ExUnit.CaptureLog

  alias Equinox.Decider
  alias Equinox.Store.State
  alias Equinox.Decider.{Decision, LoadPolicy, ResyncPolicy}
  alias Equinox.TestMocks.StoreMock


  @stream "Invoice-1"

  setup :verify_on_exit!

  # We test all versions of decider with the same test suite
  Enum.each([Decider, Decider.Async], fn decider_mod ->
    describe "#{inspect(decider_mod)}.query/2" do
      test "loads state using store, passes it to a query function and returns its result" do
        expect(StoreMock, :load, fn @stream, _, _ -> {:ok, State.new(:value, -1)} end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert :value = Decider.query(decider, & &1)
      end

      test "respects provided load policy" do
        expect(StoreMock, :load, fn _, %{assumes_empty?: true}, _ ->
          {:ok, State.new(:value, -1)}
        end)

        expect(StoreMock, :load, fn _, %{assumes_empty?: false}, _ ->
          {:ok, State.new(:value, -1)}
        end)

        decider = init(unquote(decider_mod))

        assert :value = Decider.query(decider, & &1, LoadPolicy.assume_empty())
        assert :value = Decider.query(decider, & &1, LoadPolicy.require_load())
      end

      test "crashes if store returns error" do
        expect(StoreMock, :load, fn _, _, _ -> {:error, %RuntimeError{}} end)

        decider = init(unquote(decider_mod))

        assert capture_crash(fn -> Decider.query(decider, & &1) end) =~ "RuntimeError"
      end

      test "crashes if query function raises an exception" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(:initial, -1)} end)

        decider = init(unquote(decider_mod))

        assert capture_crash(fn -> Decider.query(decider, fn _state -> raise RuntimeError end) end) =~
                 "RuntimeError"
      end
    end

    describe "#{inspect(decider_mod)}.transact/3" do
      test "executes decision callback and syncs the resulting outcome using provided store, codec and fold" do
        expect(StoreMock, :load, fn @stream, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, fn @stream, %{version: -1}, %{events: [2, 3]}, _ ->
          {:ok, State.new(5, 1)}
        end)

        decider = init(unquote(decider_mod), stream_name: @stream)

        assert :ok = Decider.transact(decider, fn 0 -> [2, 3] end)
      end

      test "respects provided load policy" do
        stub(StoreMock, :sync, fn _, %{version: -1}, _, _ -> {:ok, State.new(5, 1)} end)

        expect(StoreMock, :load, fn _, %{assumes_empty?: true}, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :load, fn _, %{assumes_empty?: false}, _ -> {:ok, State.new(0, -1)} end)

        decider = init(unquote(decider_mod))

        assert :ok = Decider.transact(decider, fn 0 -> [2] end, LoadPolicy.assume_empty())
        assert :ok = Decider.transact(decider, fn 0 -> [2] end, LoadPolicy.require_load())
      end

      test "passes optional sync context all the way to the store via decision outcome" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, fn _, %{version: -1}, %{context: %{value: 2}}, _ ->
          {:ok, State.new(5, 1)}
        end)

        decider = init(unquote(decider_mod), context: %{value: 2})

        assert :ok = Decider.transact(decider, fn 0 -> [2] end)
      end

      test "keeps track of current state version during the sync process" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(2, 100)} end)

        expect(StoreMock, :sync, fn _, %{version: 100}, %{events: [3]}, _ ->
          {:ok, State.new(5, 1)}
        end)

        decider = init(unquote(decider_mod))

        assert :ok = Decider.transact(decider, fn 2 -> 3 end)
      end

      test "handles state<->stream conflicts by reloading the state and rerunning the decision against it" do
        # Expectation:

        # 1. Initial load has no events, so state value is `0` and state version is `-1`
        expect(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)
        decider = init(unquote(decider_mod), resync: ResyncPolicy.max_attempts(1))

        # 2. In the meantime, someone else writes event `2` to the stream, stream version is now `0`

        # 3. We make the `+ 3` decision based on state value `0` (version `-1`), producing `0 + 3 = 3` event
        decision = &(&1 + 3)

        # 4. We fail to sync the result of the decision due to the version conflict (-1 != 0)
        expect(StoreMock, :sync, fn _, %{version: -1}, %{events: [3]}, _ ->
          {
            :conflict,
            # 5. We resync the stream and arrive at the new state value `2`, version `0`
            fn -> {:ok, State.new(2, 0)} end
          }
        end)

        # 6. We redo the `+ 3` decision based on the new state value `2`, producing `2 + 3 = 5` event

        # 7. We successfully sync the result of the updated decision to the stream and produce new state
        expect(StoreMock, :sync, fn _, %{version: 0}, %{events: [5]}, _ ->
          {:ok, State.new(7, 1)}
        end)

        # Execution:

        assert :ok = Decider.transact(decider, decision)
      end

      test "respects configured resync policy" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 1, fn _, %{version: -1}, _, _ ->
          {:conflict, fn -> raise RuntimeError end}
        end)

        # no resyncs allowed
        decider = init(unquote(decider_mod), resync: ResyncPolicy.max_attempts(0))

        # so we crash immediately without retrying
        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~
                 "ExhaustedResyncAttempts"
      end

      test "decision callbacks returning result and events propagate result after sync" do
        expect(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)
        expect(StoreMock, :sync, fn _, _, %{events: [1]}, _ -> {:ok, State.new(1, 0)} end)

        expect(StoreMock, :load, fn _, _, _ -> {:ok, State.new(1, 0)} end)
        expect(StoreMock, :sync, fn _, _, %{events: [2]}, _ -> {:ok, State.new(2, 1)} end)

        decider = init(unquote(decider_mod))

        assert {:ok, :one} = Decider.transact(decider, fn 0 -> {:ok, :one, [1]} end)
        assert {:ok, :two} = Decider.transact(decider, fn 1 -> {:two, [2]} end)
      end

      test "decision callbacks returning nil or empty list do not trigger sync" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 0, fn _, _, _, _ -> raise RuntimeError end)

        decider = init(unquote(decider_mod))

        assert :ok = Decider.transact(decider, fn 0 -> nil end)
        assert :ok = Decider.transact(decider, fn 0 -> [] end)
      end

      test "decision callback errors are propagated back without triggering sync" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 0, fn _, _, _, _ -> raise RuntimeError end)

        decider = init(unquote(decider_mod))

        assert {:error, %Decision.Error{term: :custom_error}} =
                 Decider.transact(decider, fn 0 -> {:error, :custom_error} end)
      end

      test "crashes if store returns error" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 1, fn _, %{version: -1}, _, _ -> {:error, %RuntimeError{}} end)

        decider = init(unquote(decider_mod))

        assert capture_crash(fn -> Decider.transact(decider, fn _ -> 3 end) end) =~ "RuntimeError"
      end

      test "crashes if store raises an exception" do
        stub(StoreMock, :load, fn _, _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 1, fn _, %{version: -1}, _, _ -> raise RuntimeError end)

        decider = init(unquote(decider_mod), max_sync_attempts: 2)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "RuntimeError"
      end

      test "crashes if decision raises an exception" do
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
    |> Decider.for_stream(
      store: {StoreMock, allow_mocks_from: self()},
      resync: Keyword.get(attrs, :resync, ResyncPolicy.max_attempts(0)),
      context: Keyword.get(attrs, :context, %{})
    )
  end

  defp init(Decider.Async, attrs) do
    init(Decider, attrs)
    |> Decider.async(
      supervisor: :disabled,
      registry: :disabled,
      context: Keyword.get(attrs, :context, %{})
    )
  end
end
