alias Equinox.UUID
alias Equinox.Events.EventData
alias MessageDb.{Writer, Reader}

stream = "benchmarkStream-" <> UUID.generate()

message_stream =
  Stream.repeatedly(fn ->
    EventData.new(
      type: "TestMessage",
      data: %{
        "first_name" => "Konstantin",
        "last_name" => "Kudryashov",
        "short_name" => "KK",
        "nickname" => "everzet"
      },
      metadata: %{
        "tenant_id" => "Wrongfast"
      }
    )
  end)

{:ok, _} = MessageDb.Connection.start_link(name: CONN)
{:ok, _} = Writer.write_messages(CONN, stream, Enum.take(message_stream, 1_000), -1)

Benchee.run(
  %{
    "Reader.get_stream_messages/4" => fn count ->
      {:ok, _} = Reader.get_stream_messages(CONN, stream, 0, count)
    end
  },
  inputs: [
    {"1 message", 1},
    {"10 messages", 10},
    {"1,000 messages", 1_000}
  ],
  time: 10
)
