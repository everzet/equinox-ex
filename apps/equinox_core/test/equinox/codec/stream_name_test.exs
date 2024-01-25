defmodule Equinox.Codec.StreamNameTest do
  use ExUnit.Case, async: true

  alias Equinox.Codec.{StreamId, StreamName}

  test "new/2" do
    assert %StreamName{
             category: "stream",
             stream_id: %StreamId{fragments: ["1"], combined: "1"},
             combined: "stream-1"
           } = StreamName.new("stream", StreamId.new("1"))

    assert_raise StreamName.Category.Error, fn -> StreamName.new("st-ream", StreamId.new("1")) end
  end

  test "encode/1" do
    assert "stream-1" =
             StreamName.encode(%StreamName{
               category: "stream",
               stream_id: %StreamId{fragments: ["1"], combined: "1"},
               combined: "stream-1"
             })
  end

  test "decode/2" do
    assert {:ok,
            %StreamName{
              category: "stream",
              stream_id: %StreamId{fragments: ["1"], combined: "1"},
              combined: "stream-1"
            }} = StreamName.decode("stream-1", 1)

    assert {:error, %StreamName.Fragments.Error{}} = StreamName.decode("stream", 1)
    assert {:error, %StreamId.Fragments.Error{}} = StreamName.decode("stream-1_2", 1)
  end

  test "decode!/1" do
    assert %StreamName{
             category: "stream",
             stream_id: %StreamId{fragments: ["1"], combined: "1"},
             combined: "stream-1"
           } = StreamName.decode!("stream-1", 1)

    assert_raise StreamName.Fragments.Error, fn -> StreamName.decode!("stream", 1) end
    assert_raise StreamId.Fragments.Error, fn -> StreamName.decode!("stream-1_2", 1) end
  end
end
