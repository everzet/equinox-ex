defmodule Equinox.MessageDb.WriterTest do
  use Equinox.MessageDb.ConnCase

  alias Equinox.Store.{DuplicateMessageId, StreamVersionConflict}
  alias Equinox.Events.EventData
  alias Equinox.MessageDb.Writer

  describe "write_messages/4" do
    @stream "testStream-42"

    test_in_isolation "successfully writing single message", %{conn: conn} do
      message = EventData.new(type: "SomeMessageType")
      assert {:ok, 0} = Writer.write_messages(conn, @stream, [message], -1)
    end

    test_in_isolation "successfully writing multiple messages", %{conn: conn} do
      message = fn -> EventData.new(type: "SomeMessageType") end
      assert {:ok, 1} = Writer.write_messages(conn, @stream, [message.(), message.()], -1)
    end

    test_in_isolation "successfully writing subsequent messages", %{conn: conn} do
      message = fn -> EventData.new(type: "SomeMessageType") end
      assert {:ok, 0} = Writer.write_messages(conn, @stream, [message.()], -1)
      assert {:ok, 2} = Writer.write_messages(conn, @stream, [message.(), message.()], 0)
      assert {:ok, 3} = Writer.write_messages(conn, @stream, [message.()], 2)
    end

    test_in_isolation "returns current version when given empty list", %{conn: conn} do
      assert {:ok, -1} = Writer.write_messages(conn, @stream, [], -1)
    end

    test_in_isolation "failing to write messages with duplicate ID", %{conn: conn} do
      message = EventData.new(type: "SomeMessageType")
      message_id = message.id

      assert {:error, %DuplicateMessageId{message_id: ^message_id}} =
               Writer.write_messages(conn, @stream, [message, message], -1)
    end

    test_in_isolation "failing to write message with wrong expected_version", %{conn: conn} do
      message = EventData.new(type: "SomeMessageType")

      assert {:error, %StreamVersionConflict{stream_name: @stream, stream_version: -1}} =
               Writer.write_messages(conn, @stream, [message], 999)
    end
  end
end
