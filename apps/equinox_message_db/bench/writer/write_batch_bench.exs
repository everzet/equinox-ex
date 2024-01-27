alias Equinox.UUID
alias Equinox.Events.EventData
alias Equinox.MessageDb.{Connection, Writer}

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
        "tenant_id" => "Equinox"
      }
    )
  end)

{:ok, _} = Connection.start_link(name: CONN)

Benchee.run(
  %{
    "Writer.write_messages/4" => fn {stream, messages, count} ->
      expected_version = -1 + count
      {:ok, ^expected_version} = Writer.write_messages(CONN, stream, messages, -1)
    end
  },
  inputs: [
    {"single writer, 1 message", 1},
    {"single writer, 5 messages", 5},
    {"single writer, 10 messages", 10},
    {"single writer, 100 messages", 100}
  ],
  before_each: fn count ->
    {"benchmarkStream-" <> UUID.generate(), message_stream |> Enum.take(count), count}
  end,
  time: 10
)
