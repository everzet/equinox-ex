defmodule Equinox.Codec.PassThroughDataTest do
  use ExUnit.Case, async: true

  alias Equinox.Events.{TimelineEvent, EventData}
  alias Equinox.Codec.PassThroughData

  describe "decode" do
    test "passes through TimelineEvent data, unchanged" do
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

      assert %{"val1" => 1, "val2" => 2} = PassThroughData.decode(event)
    end
  end

  describe "encode" do
    test "passes through data, unchanged" do
      assert %EventData{type: "Custom", data: %{"val1" => 1, "val2" => 2}} =
               PassThroughData.encode(%{"type" => "Custom", "val1" => 1, "val2" => 2}, nil)
    end

    test "raises ArgumentError if given man has no `type` field" do
      assert_raise ArgumentError, fn -> PassThroughData.encode(%{}, nil) end
    end
  end
end
