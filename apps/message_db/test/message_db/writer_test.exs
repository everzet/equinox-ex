defmodule MessageDb.WriterTest do
  use MessageDb.ConnCase
  alias MessageDb.Writer
  alias Equinox.EventData

  doctest Writer

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

      assert {:error, %Writer.DuplicateMessageId{}} =
               Writer.write_messages(conn, @stream, [message, message], -1)
    end

    test_in_isolation "failing to write message with wrong expected_version", %{conn: conn} do
      message = EventData.new(type: "SomeMessageType")

      assert {:error, %Writer.StreamVersionConflict{}} =
               Writer.write_messages(conn, @stream, [message], 999)
    end
  end
end
