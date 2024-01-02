defmodule Equinox.Codec.EventStructsTest do
  use ExUnit.Case, async: true

  alias Equinox.Events.TimelineEvent
  alias Equinox.Codec.EventStructs
  alias Equinox.CodecStubs.TestStruct
  alias Equinox.Codec.Errors

  describe "struct_to_event_data/1" do
    test "converts structs into string maps" do
      struct = %TestStruct{val1: 1, val2: 2}

      assert {:ok, event_data} = EventStructs.struct_to_event_data(struct, Equinox.CodecStubs)
      assert event_data.type == "TestStruct"
      assert event_data.data == %{"val1" => 1, "val2" => 2}
    end

    test "errors if given struct under different parent module" do
      struct = %TestStruct{val1: 1, val2: 2}
      assert {:error, %Errors.EncodeError{}} = EventStructs.struct_to_event_data(struct, Enum)
    end

    test "errors if given anything but struct" do
      assert {:error, %Errors.EncodeError{}} =
               EventStructs.struct_to_event_data(nil, Equinox.CodecStubs)

      assert {:error, %Errors.EncodeError{}} =
               EventStructs.struct_to_event_data(false, Equinox.CodecStubs)

      assert {:error, %Errors.EncodeError{}} =
               EventStructs.struct_to_event_data("str", Equinox.CodecStubs)

      assert {:error, %Errors.EncodeError{}} =
               EventStructs.struct_to_event_data(%{}, Equinox.CodecStubs)
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

      assert {:ok, %TestStruct{val1: 1, val2: 2}} =
               EventStructs.timeline_event_to_struct(event, Equinox.CodecStubs)
    end

    test "timeline_event_to_struct/1 upcasts resulting struct if it implements Upcast protocol" do
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

      assert {:ok, %TestStruct{val1: 1, val2: 3}} =
               EventStructs.timeline_event_to_struct(event, Equinox.CodecStubs)
    end

    test "timeline_event_to_struct/1 errors if struct with given type does not exist" do
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

      assert {:error, %Errors.DecodeError{}} =
               EventStructs.timeline_event_to_struct(event, Equinox.CodecStubs)
    end

    test "timeline_event_to_struct/1 errors if wrong parent module given" do
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

      assert {:error, %Errors.DecodeError{}} = EventStructs.timeline_event_to_struct(event, Enum)
    end

    test "timeline_event_to_struct/1 errors if required struct fields are missing" do
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

      assert {:error, %Errors.DecodeError{}} =
               EventStructs.timeline_event_to_struct(event, Equinox.CodecStubs)
    end
  end
end
