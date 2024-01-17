defmodule Equinox.Codec.StreamNameTest do
  use ExUnit.Case, async: true

  alias Equinox.Codec.StreamName

  test "encode/1" do
    assert "stream-1" = StreamName.encode("stream", "1")
    assert_raise StreamName.Category.Error, fn -> StreamName.encode("st-ream", "1") end
  end

  test "parse/1" do
    assert {:ok, "stream-1"} = StreamName.parse("stream-1")
    assert {:error, %StreamName.Fragments.Error{}} = StreamName.parse("stream")
  end

  test "parse!" do
    assert "stream-1" = StreamName.parse!("stream-1")
    assert_raise StreamName.Fragments.Error, fn -> StreamName.parse!("stream") end
  end

  test "decode/1" do
    assert {:ok, {"stream", "1"}} = StreamName.decode("stream-1")
    assert {:error, %StreamName.Fragments.Error{}} = StreamName.decode("stream")
  end

  test "decode!/1" do
    assert {"stream", "1"} = StreamName.decode!("stream-1")
    assert_raise StreamName.Fragments.Error, fn -> StreamName.decode!("stream") end
  end

  test "match/2" do
    assert {:ok, "1"} = StreamName.match("stream", "stream-1")
    assert {:error, %StreamName.WrongCategory{}} = StreamName.match("s", "stream-1")
    assert {:error, %StreamName.Fragments.Error{}} = StreamName.match("stream", "stream")
  end

  test "match!/2" do
    assert "1" = StreamName.match!("stream", "stream-1")
    assert_raise StreamName.WrongCategory, fn -> StreamName.match!("s", "stream-1") end
    assert_raise StreamName.Fragments.Error, fn -> StreamName.match!("stream", "stream") end
  end
end
