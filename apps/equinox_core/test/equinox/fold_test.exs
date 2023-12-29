defmodule Equinox.FoldTest do
  use ExUnit.Case, async: true

  alias Equinox.TestMocks.FoldMock
  alias Equinox.{Fold, State}

  import Mox

  setup :verify_on_exit!

  describe "fold/3" do
    test "performs reduction via Fold.evolve/2 on every given event" do
      expect(FoldMock, :evolve, fn 0, :a -> 1 end)
      expect(FoldMock, :evolve, fn 1, :b -> 2 end)

      assert Fold.fold([{:a, 0}, {:b, 1}], %State{value: 0, version: -1}, FoldMock) ==
               %State{value: 2, version: 1}
    end

    test "wraps all exceptions into FoldError" do
      expect(FoldMock, :evolve, fn _, _ -> raise RuntimeError end)

      assert_raise Fold.FoldError, ~r/runtime error/, fn ->
        Fold.fold([{:a, 0}], %State{value: 0, version: -1}, FoldMock)
      end
    end
  end

  describe "ReplaceWithLatestEvent" do
    alias Equinox.Fold.ReplaceWithLatestEvent

    test "its initial value is nil" do
      assert nil == ReplaceWithLatestEvent.initial()
    end

    test "it evolves by replacing its whole state with the latest event" do
      assert :new_event == ReplaceWithLatestEvent.evolve(:any_previous_state, :new_event)
    end
  end
end
