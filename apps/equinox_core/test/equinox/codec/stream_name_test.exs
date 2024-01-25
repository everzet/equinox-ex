defmodule Equinox.Codec.StreamNameTest do
  use ExUnit.Case, async: true

  alias Equinox.Codec.StreamName

  test "new/2" do
    assert %StreamName{category: "stream", id: "1", full: "stream-1"} =
             StreamName.new("stream", "1")

    assert_raise StreamName.Category.Error, fn -> StreamName.new("st-ream", "1") end
  end

  test "decode/1" do
    assert {:ok, %StreamName{category: "stream", id: "1", full: "stream-1"}} =
             StreamName.decode("stream-1")

    assert {:error, %StreamName.Fragments.Error{}} = StreamName.decode("stream")
  end

  test "decode!/1" do
    assert %StreamName{category: "stream", id: "1", full: "stream-1"} =
             StreamName.decode!("stream-1")

    assert_raise StreamName.Fragments.Error, fn -> StreamName.decode!("stream") end
  end
end
