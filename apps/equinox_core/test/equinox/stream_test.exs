defmodule Equinox.StreamTest do
  use ExUnit.Case, async: true
  alias Equinox.Stream.{StreamId, StreamName, Errors}

  describe "StreamId.generate/1" do
    test "supports varied number of elements" do
      assert "a-b" = StreamId.generate(["a-b"])
      assert "a_b-c" = StreamId.generate(["a", "b-c"])
    end

    test "does not allow elements to contain reserved characters" do
      assert_raise Errors.ElementError, fn -> StreamId.generate(["a_"]) end
      assert_raise Errors.ElementError, fn -> StreamId.generate(["a", "_b"]) end
    end
  end

  describe "StreamId.parse/1" do
    test "parses strings into elements" do
      assert {:ok, ["a1", "b2"]} = StreamId.parse("a1_b2")
    end

    test "works for single element ids" do
      assert {:ok, ["a"]} = StreamId.parse("a")
    end

    test "requires non-empty string" do
      assert {:error, %Errors.ElementError{}} = StreamId.parse("")
      assert {:error, %Errors.ElementError{}} = StreamId.parse(:val)
    end
  end

  describe "StreamName.generate" do
    test "combines category and stream id into a fully qualified stream name" do
      assert "a-b" = StreamName.generate("a", StreamId.generate(["b"]))
    end

    test "does not allow category names with separators in them" do
      assert_raise Errors.ElementError, fn ->
        StreamName.generate("a-", StreamId.generate(["b"]))
      end

      assert_raise Errors.ElementError, fn ->
        StreamName.generate("-a", StreamId.generate(["b"]))
      end
    end
  end

  describe "StreamName.parse/1" do
    test "parses full stream name into constituent parts" do
      assert {:ok, {"Invoice", ["A-B-C", "D-E-F"]}} = StreamName.parse("Invoice-A-B-C_D-E-F")
    end

    test "always expects category and stream id present" do
      assert {:error, %Errors.ElementError{}} = StreamName.parse("Invoice")
    end

    test "requires non-empty string" do
      assert {:error, %Errors.ElementError{}} = StreamName.parse("")
      assert {:error, %Errors.ElementError{}} = StreamName.parse(:val)
    end
  end

  describe "StreamName.match/2" do
    test "parses and returns stream id if category matches" do
      assert {:ok, ["A-B-C", "D-E-F"]} = StreamName.match("Invoice", "Invoice-A-B-C_D-E-F")
    end

    test "parses and returns error if category does not match" do
      assert {:error, %Errors.ElementError{}} = StreamName.match("Payment", "Invoice-A-B-C_D-E-F")
    end
  end
end
