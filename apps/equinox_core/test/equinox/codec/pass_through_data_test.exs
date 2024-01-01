defmodule Equinox.Codec.PassThroughDataTest do
  use ExUnit.Case, async: true

  alias Equinox.Events.{TimelineEvent, EventData}
  alias Equinox.Codec.PassThroughData

  test "passes through TimelineEvent data on decode, unchanged" do
    event =
      TimelineEvent.new(
        id: Equinox.UUID.generate(),
        type: "TestStruct",
        stream_name: "testStream-42",
        position: 0,
        global_position: 0,
        data: %{"val1" => 1, "val2" => 2},
        metadata: nil,
        time: NaiveDateTime.utc_now()
      )

    assert {:ok, %{"val1" => 1, "val2" => 2}} = PassThroughData.decode(event)
  end

  test "passes through data on encode, unchanged" do
    assert {:ok, %EventData{type: "unknown", data: %{"val1" => 1, "val2" => 2}}} =
             PassThroughData.encode(%{"val1" => 1, "val2" => 2}, nil)
  end

  test "used type field on encode, if present" do
    assert {:ok, %EventData{type: "my event"}} =
             PassThroughData.encode(%{"type" => "my event"}, nil)
  end
end
