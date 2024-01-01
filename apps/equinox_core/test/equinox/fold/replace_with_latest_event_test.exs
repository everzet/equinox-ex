defmodule Equinox.Fold.ReplaceWithLatestEventTest do
  use ExUnit.Case, async: true

  alias Equinox.Fold.ReplaceWithLatestEvent

  test "its initial value is nil" do
    assert nil == ReplaceWithLatestEvent.initial()
  end

  test "it evolves by replacing its whole state with the latest event" do
    assert :new_event == ReplaceWithLatestEvent.evolve(:any_previous_state, :new_event)
  end
end
