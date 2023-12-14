defmodule MessageDbTest do
  use ExUnit.Case
  doctest MessageDb

  test "greets the world" do
    assert MessageDb.hello() == :world
  end
end
