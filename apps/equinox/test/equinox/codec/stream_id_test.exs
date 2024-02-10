defmodule Equinox.Codec.StreamIdTest do
  use ExUnit.Case, async: true

  alias Equinox.Codec.StreamId

  describe "new/1" do
    test "encodes an ID string using exact number of fragments" do
      assert %StreamId{whole: "a"} = StreamId.new("a")
      assert %StreamId{whole: "a_b"} = StreamId.new(["a", "b"])
      assert %StreamId{whole: "a_b_c"} = StreamId.new(["a", "b", "c"])
      assert %StreamId{whole: "a_b_c_d"} = StreamId.new(["a", "b", "c", "d"])
    end

    test "raises exception if any fragment is empty" do
      assert_raise StreamId.Fragment.Error, fn -> StreamId.new("") end
      assert_raise StreamId.Fragment.Error, fn -> StreamId.new(["a", ""]) end
      assert_raise StreamId.Fragment.Error, fn -> StreamId.new(["a", "", "c"]) end
    end

    test "raises exception if any fragment includes separator character" do
      assert_raise StreamId.Fragment.Error, fn -> StreamId.new("a_") end
      assert_raise StreamId.Fragment.Error, fn -> StreamId.new(["a", "_b"]) end
      assert_raise StreamId.Fragment.Error, fn -> StreamId.new(["a", "b_", "c"]) end
    end
  end

  describe "encode/1" do
    test "simply returns `whole` part of the id" do
      id = StreamId.new(["a", "b"])
      assert "a_b" == StreamId.encode(id)
    end

    test "is used internally to convert id to string" do
      id = StreamId.new(["a", "b"])
      assert "a_b" == "#{id}"
    end
  end

  describe "decode/2" do
    test "extracts exact number of fragments from an ID string" do
      assert {:ok, %StreamId{fragments: ["a"]}} = StreamId.decode("a", 1)
      assert {:ok, %StreamId{fragments: ["a", "b"]}} = StreamId.decode("a_b", 2)
      assert {:ok, %StreamId{fragments: ["a", "b", "c"]}} = StreamId.decode("a_b_c", 3)
      assert {:ok, %StreamId{fragments: ["a", "b", "c", "d"]}} = StreamId.decode("a_b_c_d", 4)
    end

    test "returns error if number of fragments does not match specified count" do
      {:error, %StreamId.Fragments.Error{}} = StreamId.decode("a", 2)
      {:error, %StreamId.Fragments.Error{}} = StreamId.decode("a_b", 1)
      {:error, %StreamId.Fragments.Error{}} = StreamId.decode("a_b_c", 4)
    end
  end

  test "decode!/2" do
    assert %StreamId{fragments: ["a", "b"]} = StreamId.decode!("a_b", 2)
    assert_raise StreamId.Fragments.Error, fn -> StreamId.decode!("a", 2) end
    assert_raise StreamId.Fragments.Error, fn -> StreamId.decode!("a_b", 1) end
    assert_raise StreamId.Fragments.Error, fn -> StreamId.decode!("a_b_c", 4) end
  end
end
