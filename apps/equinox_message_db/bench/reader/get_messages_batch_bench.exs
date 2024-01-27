alias Equinox.UUID
alias Equinox.Events.EventData
alias Equinox.MessageDb.{Connection, Writer, Reader}

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

stream = "benchmarkStream-" <> UUID.generate()

{:ok, _} = Connection.start_link(name: CONN)
{:ok, _} = Writer.write_messages(CONN, stream, Enum.take(message_stream, 1_000), -1)

Benchee.run(
  %{
    "Reader.get_stream_messages/4" => fn count ->
      {:ok, _} = Reader.get_stream_messages(CONN, stream, 0, count)
    end
  },
  inputs: [
    {"5 messages", 5},
    {"100 messages", 100},
    {"1,000 messages", 1_000}
  ],
  time: 10
)
