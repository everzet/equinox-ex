defmodule Equinox.Store.EventsToSyncTest do
  use ExUnit.Case, async: true

  alias Equinox.Store.EventsToSync

  test "constructed via helper function new/2" do
    assert EventsToSync.new([1, 2], %{correlation: 1}) == %EventsToSync{
             events: [1, 2],
             context: %{correlation: 1}
           }

    assert EventsToSync.new([1, 2]) == %EventsToSync{events: [1, 2], context: %{}}
    assert EventsToSync.new(1) == %EventsToSync{events: [1], context: %{}}
  end

  test "can check if events are an empty list" do
    assert EventsToSync.empty?(EventsToSync.new([]))
    refute EventsToSync.empty?(EventsToSync.new([1, 2]))
  end
end
