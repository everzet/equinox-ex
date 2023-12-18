defmodule EquinoxTest do
  use ExUnit.Case
  doctest Equinox

  test "greets the world" do
    assert Equinox.hello() == :world
  end
end
