defmodule Equinox.StateTest do
  use ExUnit.Case, async: true
  alias Equinox.TestMocks.{CodecMock, FoldMock}
  alias Equinox.{Fold, State}

  import Mox

  setup :verify_on_exit!

  describe "init/1" do
    test "creates initial state with version set to -1 and value set to initial fold" do
      expect(FoldMock, :initial, fn -> :initial end)
      assert %State{value: :initial, version: -1} = State.init(FoldMock)
    end
  end

  describe "fold/3" do
    test "performs reduction via Fold.evolve/2 on every given event" do
      expect(FoldMock, :evolve, fn 0, :a -> 1 end)
      expect(FoldMock, :evolve, fn 1, :b -> 2 end)

      assert State.fold(%State{value: 0, version: -1}, [{:a, 0}, {:b, 1}], FoldMock) ==
               %State{value: 2, version: 1}
    end

    test "wraps all exceptions into FoldError" do
      expect(FoldMock, :evolve, fn _, _ -> raise RuntimeError end)

      assert_raise Fold.FoldError, ~r/runtime error/, fn ->
        State.fold(%State{value: 0, version: -1}, [{:a, 0}], FoldMock)
      end
    end
  end

  describe "load!/4" do
    test "fetches events, decodes them and then folds them into the state" do
      state = %State{value: 0, version: 2}
      fetch = fn -> [%{v: 1, position: 3}, %{v: 2, position: 4}] end

      expect(CodecMock, :decode, fn %{v: 1, position: 3} -> {:ok, :one} end)
      expect(CodecMock, :decode, fn %{v: 2, position: 4} -> {:ok, :two} end)

      expect(FoldMock, :evolve, fn 0, :one -> 1 end)
      expect(FoldMock, :evolve, fn 1, :two -> 2 end)

      assert State.load!(state, CodecMock, FoldMock, fetch) ==
               %State{value: 2, version: 4}
    end
  end

  describe "sync!/6" do
    test "encodes events, writes them, then folds them back into the state" do
      state = %State{value: 0, version: 2}
      events = [:one, :two]

      expect(CodecMock, :encode, fn :one, :ctx -> {:ok, 1} end)
      expect(CodecMock, :encode, fn :two, :ctx -> {:ok, 2} end)

      write = fn encoded_events ->
        assert encoded_events == [1, 2]
        4
      end

      expect(FoldMock, :evolve, fn 0, :one -> 1 end)
      expect(FoldMock, :evolve, fn 1, :two -> 2 end)

      assert State.sync!(state, events, :ctx, CodecMock, FoldMock, write) ==
               %State{value: 2, version: 4}
    end
  end
end
