defmodule Equinox.CodecTest do
  use ExUnit.Case, async: true
  alias Equinox.Events.TimelineEvent
  alias Equinox.Codec.{EventStructs, CodecError}

  defmodule TestStruct do
    @enforce_keys [:val1]
    defstruct [:val1, :val2]
  end

  describe "EventStructs.struct_to_event_data/1" do
    test "converts structs into string maps" do
      struct = %TestStruct{val1: 1, val2: 2}

      assert event_data = EventStructs.struct_to_event_data(struct, __MODULE__)
      assert event_data.type == "TestStruct"
      assert event_data.data == %{"val1" => 1, "val2" => 2}
    end

    test "errors if given struct under different parent module" do
      struct = %TestStruct{val1: 1, val2: 2}
      assert_raise CodecError, fn -> EventStructs.struct_to_event_data(struct, Enum) end
    end

    test "errors if given anything but struct" do
      assert_raise CodecError, fn -> EventStructs.struct_to_event_data(nil, __MODULE__) end
      assert_raise CodecError, fn -> EventStructs.struct_to_event_data(false, __MODULE__) end
      assert_raise CodecError, fn -> EventStructs.struct_to_event_data("str", __MODULE__) end
      assert_raise CodecError, fn -> EventStructs.struct_to_event_data(%{}, __MODULE__) end
    end
  end

  describe "EventStructs.timeline_event_to_struct/2" do
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
               EventStructs.timeline_event_to_struct(event, __MODULE__)
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

      assert_raise CodecError, fn ->
        EventStructs.timeline_event_to_struct(event, __MODULE__)
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

      assert_raise CodecError, fn -> EventStructs.timeline_event_to_struct(event, Enum) end
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

      assert_raise CodecError, fn -> EventStructs.timeline_event_to_struct(event, __MODULE__) end
    end
  end
end
