defmodule Equinox.Codec.StreamIdTest do
  use ExUnit.Case, async: true

  alias Equinox.Codec.StreamId

  describe "encode/1" do
    test "encodes an ID string using exact number of fragments" do
      assert "a" = StreamId.encode("a")
      assert "a" = StreamId.encode({"a"})
      assert "a_b" = StreamId.encode({"a", "b"})
      assert "a_b_c" = StreamId.encode({"a", "b", "c"})
      assert "a_b_c_d" = StreamId.encode({"a", "b", "c", "d"})
    end

    test "raises exception if any fragment is empty" do
      assert_raise StreamId.Fragment.Error, fn -> StreamId.encode("") end
      assert_raise StreamId.Fragment.Error, fn -> StreamId.encode({"a", ""}) end
      assert_raise StreamId.Fragment.Error, fn -> StreamId.encode({"a", "", "c"}) end
    end

    test "raises exception if any fragment includes separator character" do
      assert_raise StreamId.Fragment.Error, fn -> StreamId.encode("a_") end
      assert_raise StreamId.Fragment.Error, fn -> StreamId.encode({"a", "_b"}) end
      assert_raise StreamId.Fragment.Error, fn -> StreamId.encode({"a", "b_", "c"}) end
    end
  end

  describe "decode/2" do
    test "extracts exact number of fragments from an ID string" do
      assert {:ok, "a"} = StreamId.decode("a", 1)
      assert {:ok, {"a", "b"}} = StreamId.decode("a_b", 2)
      assert {:ok, {"a", "b", "c"}} = StreamId.decode("a_b_c", 3)
      assert {:ok, {"a", "b", "c", "d"}} = StreamId.decode("a_b_c_d", 4)
    end

    test "returns error if number of fragments does not match specified count" do
      {:error, %StreamId.Fragments.Error{}} = StreamId.decode("a", 2)
      {:error, %StreamId.Fragments.Error{}} = StreamId.decode("a_b", 1)
      {:error, %StreamId.Fragments.Error{}} = StreamId.decode("a_b_c", 4)
    end
  end

  test "decode!/2" do
    assert {"a", "b"} = StreamId.decode!("a_b", 2)
    assert_raise StreamId.Fragments.Error, fn -> StreamId.decode!("a", 2) end
    assert_raise StreamId.Fragments.Error, fn -> StreamId.decode!("a_b", 1) end
    assert_raise StreamId.Fragments.Error, fn -> StreamId.decode!("a_b_c", 4) end
  end
end
