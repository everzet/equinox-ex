alias MessageDb.UUID
alias MessageDb.Writer

message_stream =
  Stream.repeatedly(fn ->
    Writer.Message.new(
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

Benchee.run(
  %{
    "Writer.write_messages/4" => fn {stream, messages, count} ->
      expected_version = -1 + count
      {:ok, ^expected_version} = Writer.write_messages(CONN, stream, messages, -1)
    end
  },
  inputs: [
    {"1 message", 1},
    {"10 messages", 10},
    {"1,000 messages", 1_000},
    {"10,000 messages", 10_000}
  ],
  before_each: fn count ->
    {"benchmarkStream-" <> UUID.generate(), Enum.take(message_stream, count), count}
  end,
  time: 10
)
