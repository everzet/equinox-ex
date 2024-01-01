defmodule Equinox.CodecTest do
  use ExUnit.Case, async: true
  alias Equinox.TestMocks.CodecMock
  alias Equinox.Events.{TimelineEvent, EventData}
  alias Equinox.Codec.{EventStructs, CodecError}
  alias Equinox.Codec

  import Mox

  setup :verify_on_exit!

  describe "encode!/3" do
    test "performs Codec.encode/2 on every given event" do
      expect(CodecMock, :encode, fn 1, %{c: 1} -> {:ok, :one} end)
      expect(CodecMock, :encode, fn 2, %{c: 1} -> {:ok, :two} end)
      assert Codec.encode!([1, 2], %{c: 1}, CodecMock) == [:one, :two]
    end

    test "raises exception if Codec returns {:error, exception}" do
      expect(CodecMock, :encode, fn _, _ -> {:error, %Codec.CodecError{message: "bang"}} end)
      assert_raise Codec.CodecError, ~r/bang/, fn -> Codec.encode!([1], %{c: 1}, CodecMock) end
    end

    test "raises exception if Codec returns {:error, term}" do
      expect(CodecMock, :encode, fn _, _ -> {:error, :bang} end)
      assert_raise Codec.CodecError, ~r/:bang/, fn -> Codec.encode!([1], %{c: 1}, CodecMock) end
    end

    test "wraps all exceptions into CodecError" do
      expect(CodecMock, :encode, fn _, _ -> raise RuntimeError end)

      assert_raise Codec.CodecError, ~r/runtime error/, fn ->
        Codec.encode!([1], %{c: 1}, CodecMock)
      end
    end
  end

  describe "decode_with_position!/2" do
    test "performs Codec.decode/1 on every given event with position" do
      expect(CodecMock, :decode, fn %{v: :one, position: 3} -> {:ok, 1} end)
      expect(CodecMock, :decode, fn %{v: :two, position: 4} -> {:ok, 2} end)

      result =
        Codec.decode_with_position!(
          [%{v: :one, position: 3}, %{v: :two, position: 4}],
          CodecMock
        )

      assert Enum.to_list(result) == [{1, 3}, {2, 4}]
    end

    test "works with streams" do
      expect(CodecMock, :decode, fn %{v: :one, position: 3} -> {:ok, 1} end)
      expect(CodecMock, :decode, fn %{v: :two, position: 4} -> {:ok, 2} end)

      result =
        Codec.decode_with_position!(
          Stream.map([%{v: :one, position: 3}, %{v: :two, position: 4}], & &1),
          CodecMock
        )

      assert Enum.to_list(result) == [{1, 3}, {2, 4}]
    end

    test "raises exception if Codec returns {:error, exception}" do
      expect(CodecMock, :decode, fn _ -> {:error, %Codec.CodecError{message: "bang"}} end)

      assert_raise Codec.CodecError, ~r/bang/, fn ->
        Stream.run(Codec.decode_with_position!([%{v: :one, position: 0}], CodecMock))
      end
    end

    test "raises exception if Codec returns {:error, term}" do
      expect(CodecMock, :decode, fn _ -> {:error, :bang} end)

      assert_raise Codec.CodecError, ~r/:bang/, fn ->
        Stream.run(Codec.decode_with_position!([%{v: :one, position: 0}], CodecMock))
      end
    end

    test "wraps all exceptions into CodecError" do
      expect(CodecMock, :decode, fn _ -> raise RuntimeError end)

      assert_raise Codec.CodecError, ~r/runtime error/, fn ->
        Stream.run(Codec.decode_with_position!([%{v: :one, position: 0}], CodecMock))
      end
    end
  end

  describe "PassThroughData" do
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

      assert {:ok, %{"val1" => 1, "val2" => 2}} = Codec.PassThroughData.decode(event)
    end

    test "passes through data on encode, unchanged" do
      assert {:ok, %EventData{type: "unknown", data: %{"val1" => 1, "val2" => 2}}} =
               Codec.PassThroughData.encode(%{"val1" => 1, "val2" => 2}, nil)
    end

    test "used type field on encode, if present" do
      assert {:ok, %EventData{type: "my event"}} =
               Codec.PassThroughData.encode(%{"type" => "my event"}, nil)
    end
  end

  describe "EventStructs" do
    alias Equinox.CodecStubs.{TestStruct}

    test "struct_to_event_data/1 converts structs into string maps" do
      struct = %TestStruct{val1: 1, val2: 2}

      assert {:ok, event_data} = EventStructs.struct_to_event_data(struct, Equinox.CodecStubs)
      assert event_data.type == "TestStruct"
      assert event_data.data == %{"val1" => 1, "val2" => 2}
    end

    test "struct_to_event_data/1 errors if given struct under different parent module" do
      struct = %TestStruct{val1: 1, val2: 2}
      assert {:error, %CodecError{}} = EventStructs.struct_to_event_data(struct, Enum)
    end

    test "struct_to_event_data/1 errors if given anything but struct" do
      assert {:error, %CodecError{}} = EventStructs.struct_to_event_data(nil, Equinox.CodecStubs)

      assert {:error, %CodecError{}} =
               EventStructs.struct_to_event_data(false, Equinox.CodecStubs)

      assert {:error, %CodecError{}} =
               EventStructs.struct_to_event_data("str", Equinox.CodecStubs)

      assert {:error, %CodecError{}} = EventStructs.struct_to_event_data(%{}, Equinox.CodecStubs)
    end

    test "timeline_event_to_struct/1 converts timeline event into existing struct under specified module" do
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

      assert {:error, %CodecError{}} =
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

      assert {:error, %CodecError{}} = EventStructs.timeline_event_to_struct(event, Enum)
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

      assert {:error, %CodecError{}} =
               EventStructs.timeline_event_to_struct(event, Equinox.CodecStubs)
    end
  end
end
