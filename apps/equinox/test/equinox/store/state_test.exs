defmodule Equinox.Store.StateTest do
  use ExUnit.Case, async: true

  alias Equinox.Store.State

  test "constructed via helper function new/2" do
    assert State.new("A", 10) == %State{value: "A", version: 10}
  end
end
