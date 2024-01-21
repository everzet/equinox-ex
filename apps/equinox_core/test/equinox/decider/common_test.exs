defmodule Equinox.Decider.CommonTest do
  use ExUnit.Case, async: true

  import Mox
  import ExUnit.CaptureLog

  alias Equinox.Decider
  alias Equinox.Store.State
  alias Equinox.Decider.{Decision, LoadPolicy, ResyncPolicy}
  alias Equinox.StoreMock

  @stream "Invoice-1"

  setup :verify_on_exit!

  # We test all versions of decider with the same test suite
  Enum.each([Decider, Decider.Async], fn decider_mod ->
    describe "#{inspect(decider_mod)}.query/2" do
      test "loads state using store, passes it to a query callback and returns its result" do
        decider = init(unquote(decider_mod), stream_name: @stream)
        expect(StoreMock, :load, fn @stream, _load_policy -> {:ok, State.new(:value, -1)} end)
        assert :value = Decider.query(decider, & &1)
      end

      test "respects default load policy set during init" do
        decider = init(unquote(decider_mod), load: LoadPolicy.assume_empty())
        expect(StoreMock, :load, fn _, %{assumes_empty?: true} -> {:ok, State.new(:val, -1)} end)
        assert :val = Decider.query(decider, & &1)

        decider = init(unquote(decider_mod), load: LoadPolicy.require_load())
        expect(StoreMock, :load, fn _, %{assumes_empty?: false} -> {:ok, State.new(:val, -1)} end)
        assert :val = Decider.query(decider, & &1)
      end

      test "allows default load policy to be overriden with optional third argument" do
        decider = init(unquote(decider_mod), load: LoadPolicy.assume_empty())
        expect(StoreMock, :load, fn _, %{assumes_empty?: false} -> {:ok, State.new(:val, -1)} end)
        assert :val = Decider.query(decider, & &1, LoadPolicy.require_load())
      end

      test "crashes if store returns error" do
        decider = init(unquote(decider_mod))
        expect(StoreMock, :load, fn _, _ -> {:error, %RuntimeError{}} end)
        assert capture_crash(fn -> Decider.query(decider, & &1) end) =~ "RuntimeError"
      end

      test "crashes if query callback raises an exception" do
        decider = init(unquote(decider_mod))
        stub(StoreMock, :load, fn _, _ -> {:ok, State.new(:initial, -1)} end)

        assert capture_crash(fn -> Decider.query(decider, fn _ -> raise RuntimeError end) end) =~
                 "RuntimeError"
      end
    end

    describe "#{inspect(decider_mod)}.transact/3" do
      test "executes decision callback and syncs the resulting events via configured store" do
        decider = init(unquote(decider_mod), stream_name: @stream)

        expect(StoreMock, :load, fn @stream, _load_policy -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, fn @stream, %{version: -1}, %{events: [2, 3]} ->
          {:ok, State.new(5, 1)}
        end)

        assert :ok = Decider.transact(decider, fn 0 -> [2, 3] end)
      end

      test "respects default load policy set during init" do
        stub(StoreMock, :sync, fn _, %{version: -1}, _ -> {:ok, State.new(5, 1)} end)

        decider = init(unquote(decider_mod), load: LoadPolicy.assume_empty())
        expect(StoreMock, :load, fn _, %{assumes_empty?: true} -> {:ok, State.new(0, -1)} end)
        assert :ok = Decider.transact(decider, fn 0 -> [2] end)

        decider = init(unquote(decider_mod), laod: LoadPolicy.require_load())
        expect(StoreMock, :load, fn _, %{assumes_empty?: false} -> {:ok, State.new(0, -1)} end)
        assert :ok = Decider.transact(decider, fn 0 -> [2] end)
      end

      test "allows default load policy to be overriden with optional third argument" do
        decider = init(unquote(decider_mod), load: LoadPolicy.assume_empty())
        stub(StoreMock, :sync, fn _, %{version: -1}, _ -> {:ok, State.new(5, 1)} end)

        expect(StoreMock, :load, fn _, %{assumes_empty?: false} -> {:ok, State.new(0, -1)} end)
        assert :ok = Decider.transact(decider, fn 0 -> [2] end, LoadPolicy.require_load())
      end

      test "when provided, passes wrapper context all the way to the Store.sync/3" do
        decider = init(unquote(decider_mod))
        stub(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, fn _, %{version: -1}, %{context: %{value: :ctx}} ->
          {:ok, State.new(5, 1)}
        end)

        assert :ok = Decider.transact(decider, {fn 0 -> [2] end, %{value: :ctx}})
      end

      test "keeps track of current state version during the sync process" do
        decider = init(unquote(decider_mod))
        stub(StoreMock, :load, fn _, _ -> {:ok, State.new(2, 100)} end)

        expect(StoreMock, :sync, fn _, %{version: 100}, %{events: [3]} ->
          {:ok, State.new(5, 1)}
        end)

        assert :ok = Decider.transact(decider, fn 2 -> 3 end)
      end

      test "handles state<->stream conflicts by reloading the state and rerunning the decision against it" do
        decider = init(unquote(decider_mod), resync: ResyncPolicy.max_attempts(1))

        # Expectation:

        # 1. Initial load has no events, so state value is `0` and state version is `-1`
        expect(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

        # 2. In the meantime, someone else writes event `2` to the stream, stream version is now `0`
        # ...

        # 3. We make `+ 3` decision on the state `0` (version `-1`), producing event `3` (`0 + 3 = 3`)
        decision = &(&1 + 3)

        # 4. We fail to sync the decision result due to the state-stream version conflict (-1 != 0)
        expect(StoreMock, :sync, fn _, %{version: -1}, %{events: [3]} ->
          {
            :conflict,
            # 4.1 Store helpfully returns resync callback alongside with the conflict
            fn -> {:ok, State.new(2, 0)} end
          }
        end)

        # 5. We resync the stream using provided callback and arrive at the new state value `2`, version `0`
        # ...

        # 6. We redo the `+ 3` decision based on the new state value `2`, producing `2 + 3 = 5` event
        # &decision

        # 7. We successfully sync the result of the updated decision to the stream and produce new state
        expect(StoreMock, :sync, fn _, %{version: 0}, %{events: [5]} ->
          {:ok, State.new(7, 1)}
        end)

        # Execution:

        assert :ok = Decider.transact(decider, decision)
      end

      test "respects configured resync policy" do
        decider = init(unquote(decider_mod), resync: ResyncPolicy.max_attempts(0))
        stub(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 1, fn _, %{version: -1}, _ ->
          {:conflict, fn -> raise RuntimeError end}
        end)

        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~
                 "ExhaustedResyncAttempts"
      end

      test "decision callbacks returning result and events propagate result after sync" do
        decider = init(unquote(decider_mod))

        expect(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)
        expect(StoreMock, :sync, fn _, _, %{events: [1]} -> {:ok, State.new(1, 0)} end)
        assert {:ok, :one} = Decider.transact(decider, fn 0 -> {:ok, :one, [1]} end)

        expect(StoreMock, :load, fn _, _ -> {:ok, State.new(1, 0)} end)
        expect(StoreMock, :sync, fn _, _, %{events: [2]} -> {:ok, State.new(2, 1)} end)
        assert {:ok, :two} = Decider.transact(decider, fn 1 -> {:two, [2]} end)
      end

      test "decision callbacks returning nil or empty list do not trigger sync" do
        decider = init(unquote(decider_mod))
        stub(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

        assert :ok = Decider.transact(decider, fn 0 -> nil end)
        assert :ok = Decider.transact(decider, fn 0 -> [] end)
        assert :ok = Decider.transact(decider, fn 0 -> {:ok, nil} end)
        assert :ok = Decider.transact(decider, fn 0 -> {:ok, []} end)
        assert {:ok, :res} = Decider.transact(decider, fn 0 -> {:res, nil} end)
        assert {:ok, :res} = Decider.transact(decider, fn 0 -> {:res, []} end)
        assert {:ok, :res} = Decider.transact(decider, fn 0 -> {:ok, :res, nil} end)
        assert {:ok, :res} = Decider.transact(decider, fn 0 -> {:ok, :res, []} end)
      end

      test "decision callback errors are wrapped into Decision.Error and propagated back without sync" do
        decider = init(unquote(decider_mod))
        stub(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

        assert {:error, %Decision.Error{}} = Decider.transact(decider, fn 0 -> {:error, :err} end)
      end

      test "decision callback errors can be terms, exceptions, Decision.Error instances or strings" do
        decider = init(unquote(decider_mod))
        stub(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

        assert {:error, %Decision.Error{message: ":er", exception: nil, term: :er}} =
                 Decider.transact(decider, fn 0 -> {:error, :er} end)

        assert {:error, %Decision.Error{message: "er", exception: %RuntimeError{}, term: nil}} =
                 Decider.transact(decider, fn 0 -> {:error, RuntimeError.exception("er")} end)

        assert {:error, %Decision.Error{message: "er", exception: nil, term: nil}} =
                 Decider.transact(decider, fn 0 -> {:error, Decision.Error.exception("er")} end)

        assert {:error, %Decision.Error{message: "er", exception: nil, term: nil}} =
                 Decider.transact(decider, fn 0 -> {:error, "er"} end)
      end

      test "crashes if decision raises an exception" do
        decider = init(unquote(decider_mod))
        stub(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

        assert capture_crash(fn -> Decider.transact(decider, fn _ -> raise RuntimeError end) end) =~
                 "RuntimeError"
      end

      test "crashes if store returns error" do
        decider = init(unquote(decider_mod))
        stub(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 1, fn _, %{version: -1}, _ -> {:error, %RuntimeError{}} end)
        assert capture_crash(fn -> Decider.transact(decider, fn _ -> 3 end) end) =~ "RuntimeError"
      end

      test "crashes if store raises an exception" do
        decider = init(unquote(decider_mod), max_sync_attempts: 2)
        stub(StoreMock, :load, fn _, _ -> {:ok, State.new(0, -1)} end)

        expect(StoreMock, :sync, 1, fn _, %{version: -1}, _ -> raise RuntimeError end)
        assert capture_crash(fn -> Decider.transact(decider, & &1) end) =~ "RuntimeError"
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
      store: %StoreMock.Config{allow_from: self()},
      load: Keyword.get(attrs, :load, LoadPolicy.require_load()),
      resync: Keyword.get(attrs, :resync, ResyncPolicy.max_attempts(0))
    )
  end

  defp init(Decider.Async, attrs) do
    init(Decider, attrs)
    |> Decider.async(
      supervisor: :disabled,
      registry: :disabled
    )
  end
end
