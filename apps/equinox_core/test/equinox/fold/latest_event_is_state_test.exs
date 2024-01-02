defmodule Equinox.Fold.LatestEventIsStateTest do
  use ExUnit.Case, async: true

  alias Equinox.Fold.LatestEventIsState

  test "its initial value is nil" do
    assert nil == LatestEventIsState.initial()
  end

  test "it evolves by replacing its whole state with the latest event" do
    assert :new_event == LatestEventIsState.evolve(:any_previous_state, :new_event)
  end
end
