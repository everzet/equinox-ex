defmodule Equinox.Codec.EventStructsTest do
  use ExUnit.Case, async: true

  alias Equinox.Events.{EventData, TimelineEvent}
  alias Equinox.Codec.EventStructs
  alias Equinox.CodecStubs.{TestStruct, DowncastableTestStruct}

  describe "struct_to_event_data/1" do
    test "converts structs into string maps" do
      struct = %TestStruct{val1: 1, val2: 2}

      assert %EventData{type: "TestStruct", data: %{"val1" => 1, "val2" => 2}} =
               EventStructs.struct_to_event_data(struct, Equinox.CodecStubs)
    end

    test "downcasts struct before encoding if it implements Downcast protocol" do
      struct = %DowncastableTestStruct{val1: 1, val2: 2}

      assert %EventData{data: %{"val2" => 1}} =
               EventStructs.struct_to_event_data(struct, Equinox.CodecStubs)
    end

    test "errors if given struct under different parent module" do
      struct = %TestStruct{val1: 1, val2: 2}
      assert_raise ArgumentError, fn -> EventStructs.struct_to_event_data(struct, Enum) end
    end

    test "errors if given anything but struct" do
      assert_raise FunctionClauseError, fn ->
        EventStructs.struct_to_event_data(nil, Equinox.CodecStubs)
      end

      assert_raise FunctionClauseError, fn ->
        EventStructs.struct_to_event_data(false, Equinox.CodecStubs)
      end

      assert_raise FunctionClauseError, fn ->
        EventStructs.struct_to_event_data("str", Equinox.CodecStubs)
      end

      assert_raise FunctionClauseError, fn ->
        EventStructs.struct_to_event_data(%{}, Equinox.CodecStubs)
      end
    end
  end

  describe "timeline_event_to_struct/1" do
    test "converts timeline event into existing struct under specified module" do
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

      assert %TestStruct{val1: 1, val2: 2} =
               EventStructs.timeline_event_to_struct(event, Equinox.CodecStubs)
    end

    test "upcasts resulting struct if it implements Upcast protocol" do
      event =
        TimelineEvent.new(
          id: Equinox.UUID.generate(),
          type: "UpcastableTestStruct",
          stream_name: "testStream-42",
          position: 0,
          global_position: 0,
          data: %{"val1" => 1, "val2" => 2},
          metadata: nil,
          time: NaiveDateTime.utc_now()
        )

      assert %TestStruct{val1: 1, val2: 3} =
               EventStructs.timeline_event_to_struct(event, Equinox.CodecStubs)
    end

    test "errors if struct with given type does not exist" do
      event =
        TimelineEvent.new(
          id: Equinox.UUID.generate(),
          type: "InexistentStruct",
          stream_name: "testStream-42",
          position: 0,
          global_position: 0,
          data: %{"val1" => 1, "val2" => 2},
          metadata: nil,
          time: NaiveDateTime.utc_now()
        )

      assert_raise ArgumentError, fn ->
        EventStructs.timeline_event_to_struct(event, Equinox.CodecStubs)
      end
    end

    test "errors if wrong parent module given" do
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

      assert_raise ArgumentError, fn -> EventStructs.timeline_event_to_struct(event, Enum) end
    end

    test "errors if required struct fields are missing" do
      event =
        TimelineEvent.new(
          id: Equinox.UUID.generate(),
          type: "TestStruct",
          stream_name: "testStream-42",
          position: 0,
          global_position: 0,
          data: %{"val2" => 2},
          metadata: nil,
          time: NaiveDateTime.utc_now()
        )

      assert_raise ArgumentError, fn ->
        EventStructs.timeline_event_to_struct(event, Equinox.CodecStubs)
      end
    end

    test "errors if unknown fields are present" do
      event =
        TimelineEvent.new(
          id: Equinox.UUID.generate(),
          type: "TestStruct",
          stream_name: "testStream-42",
          position: 0,
          global_position: 0,
          data: %{"val1" => 1, "val2" => 2, "unknown_val3" => 3},
          metadata: nil,
          time: NaiveDateTime.utc_now()
        )

      assert_raise ArgumentError, fn ->
        EventStructs.timeline_event_to_struct(event, Equinox.CodecStubs)
      end
    end
  end
end
