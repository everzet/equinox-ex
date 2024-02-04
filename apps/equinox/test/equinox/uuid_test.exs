defmodule Equinox.UuidTest do
  use ExUnit.Case, async: true
  alias Equinox.UUID

  describe "parse/1" do
    test "validates against non-uuids" do
      assert {:error, %ArgumentError{}} = UUID.parse("random-string")
      assert {:error, %ArgumentError{}} = UUID.parse("")
    end

    test "parses uuids it itself generates" do
      uuid = UUID.generate()
      assert {:ok, ^uuid} = UUID.parse(uuid)
    end

    test "normalizes (downcases) given uuids" do
      assert {:ok, "13fa10db-2e23-40ea-871b-d240b1622bc8"} =
               UUID.parse("13FA10DB-2E23-40EA-871B-D240B1622BC8")
    end
  end
end
