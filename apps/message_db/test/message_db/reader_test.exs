defmodule MessageDb.ReaderTest do
  use MessageDb.ConnCase
  alias MessageDb.{Reader, Writer}

  doctest Reader

  describe "get_last_stream_message/2" do
    @stream "testStream-42"

    test_in_isolation "successfully reading back written message", %{conn: conn} do
      message =
        Writer.Message.new(
          id: MessageDb.UUID.generate(),
          type: "SomeMessageType",
          data: %{"test" => "value2"},
          metadata: %{"meta" => "value"}
        )

      assert {:ok, 0} = Writer.write_messages(conn, @stream, [message], -1)
      assert {:ok, %Reader.Message{} = written} = Reader.get_last_stream_message(conn, @stream)

      assert written.id == message.id
      assert written.type == message.type
      assert written.position == 0
      assert written.data == message.data
      assert written.metadata == message.metadata
      assert written.time
    end

    test_in_isolation "getting nil if stream is empty", %{conn: conn} do
      assert {:ok, nil} = Reader.get_last_stream_message(conn, @stream)
    end
  end

  describe "get_stream_messages/4" do
    @stream "testStream-42"
    @messages Stream.repeatedly(fn -> Writer.Message.new(type: "SomeMessageType") end)
              |> Enum.take(4)

    test_in_isolation "retrieving all of the written messages", %{conn: conn} do
      assert {:ok, 3} = Writer.write_messages(conn, @stream, @messages, -1)

      assert {:ok, messages} = Reader.get_stream_messages(conn, @stream, 0, 4)
      assert length(messages) == 4
    end

    test_in_isolation "retrieving some of the written messages", %{conn: conn} do
      assert {:ok, 3} = Writer.write_messages(conn, @stream, @messages, -1)

      assert {:ok, messages} = Reader.get_stream_messages(conn, @stream, 0, 2)
      assert length(messages) == 2
    end

    test_in_isolation "retrieving different batches", %{conn: conn} do
      assert {:ok, 3} = Writer.write_messages(conn, @stream, @messages, -1)
      first_message_id = @messages |> List.first() |> Map.get(:id)
      last_message_id = @messages |> List.last() |> Map.get(:id)

      assert {:ok, [%Reader.Message{id: ^first_message_id}]} =
               Reader.get_stream_messages(conn, @stream, 0, 1)

      assert {:ok, [%Reader.Message{id: ^last_message_id}]} =
               Reader.get_stream_messages(conn, @stream, 3, 1)
    end

    test_in_isolation "getting empty list if no messages in a stream", %{conn: conn} do
      assert {:ok, []} = Reader.get_stream_messages(conn, @stream, 0, 4)
    end
  end

  describe "stream_stream_messages/4" do
    @stream "testStream-42"
    @messages Stream.repeatedly(fn -> Writer.Message.new(type: "SomeMessageType") end)
              |> Enum.take(10)

    test_in_isolation "streaming all written messages in one large batch", %{conn: conn} do
      assert {:ok, 9} = Writer.write_messages(conn, @stream, @messages, -1)

      messages = conn |> Reader.stream_stream_messages(@stream, 0, 10) |> Enum.to_list()

      assert length(messages) == 10
      assert List.first(messages).id == List.first(@messages).id
      assert List.last(messages).id == List.last(@messages).id
    end

    test_in_isolation "streaming all written messages in batches of 1", %{conn: conn} do
      assert {:ok, 9} = Writer.write_messages(conn, @stream, @messages, -1)

      messages = conn |> Reader.stream_stream_messages(@stream, 0, 1) |> Enum.to_list()

      assert length(messages) == 10
      assert List.first(messages).id == List.first(@messages).id
      assert List.last(messages).id == List.last(@messages).id
    end

    test_in_isolation "streaming all written messages in batches of 3", %{conn: conn} do
      assert {:ok, 9} = Writer.write_messages(conn, @stream, @messages, -1)

      messages = conn |> Reader.stream_stream_messages(@stream, 0, 3) |> Enum.to_list()

      assert length(messages) == 10
      assert List.first(messages).id == List.first(@messages).id
      assert List.last(messages).id == List.last(@messages).id
    end
  end
end
