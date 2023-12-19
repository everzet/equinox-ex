defmodule Equinox.StreamTest do
  use ExUnit.Case, async: true
  alias Equinox.Stream.{StreamId, Category, StreamName, ElementError}

  describe "StreamId.new/1" do
    test "supports varied number of elements" do
      assert %StreamId{elements: ["a-b"]} = StreamId.new(["a-b"])
      assert %StreamId{elements: ["a", "b-c"]} = StreamId.new(["a", "b-c"])
    end

    test "does not allow elements to contain reserved characters" do
      assert_raise ElementError, fn -> StreamId.new(["a_"]) end
      assert_raise ElementError, fn -> StreamId.new(["a", "_b"]) end
    end

    test "implicitly convertable to string" do
      assert "#{StreamId.new(["a-1", "b-2"])}" == "a-1_b-2"
    end
  end

  describe "StreamId.parse/1" do
    test "parses strings into StreamId" do
      assert {:ok, %StreamId{elements: ["a1", "b2"]}} = StreamId.parse("a1_b2")
    end

    test "works for single element ids" do
      assert {:ok, %StreamId{elements: ["a"]}} = StreamId.parse("a")
    end
  end

  describe "Category.new" do
    test "simply wraps given name" do
      assert %Category{name: "a"} = Category.new("a")
    end

    test "does not allow name to contain reserved characters" do
      assert_raise ElementError, fn -> Category.new("a-") end
      assert_raise ElementError, fn -> Category.new("-a") end
    end

    test "implicitly convertable to string" do
      assert "#{Category.new("a")}" == "a"
    end
  end

  describe "Category.parse/1" do
    test "wraps name string into Category" do
      assert {:ok, %Category{name: "a"}} = Category.parse("a")
    end
  end

  describe "StreamName.new" do
    test "simply wraps given category and stream id" do
      assert %StreamName{category: %Category{name: "a"}, stream_id: %StreamId{elements: ["b"]}} =
               StreamName.new(Category.new("a"), StreamId.new(["b"]))
    end

    test "implicitly convertable to string" do
      assert "#{StreamName.new(Category.new("a"), StreamId.new(["b", "c"]))}" == "a-b_c"
    end
  end

  describe "StreamName.parse/1" do
    test "parses full stream name into constituent parts" do
      assert {:ok,
              %StreamName{
                category: %Category{name: "Invoice"},
                stream_id: %StreamId{elements: ["A-B-C", "D-E-F"]}
              }} = StreamName.parse("Invoice-A-B-C_D-E-F")
    end

    test "always expects category and stream id present" do
      assert {:error, %ElementError{}} = StreamName.parse("Invoice")
    end
  end

  describe "StreamName.match/2" do
    test "parses and returns okay if category matches" do
      assert {:ok, %StreamName{}} =
               StreamName.match(Category.new("Invoice"), "Invoice-A-B-C_D-E-F")
    end

    test "parses and returns error if category does not match" do
      assert {:error, %ElementError{}} =
               StreamName.match(Category.new("Payment"), "Invoice-A-B-C_D-E-F")
    end
  end
end
