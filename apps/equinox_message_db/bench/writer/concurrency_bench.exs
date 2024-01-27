alias Equinox.UUID
alias Equinox.Events.EventData
alias Equinox.MessageDb.{Connection, Writer}

batch_size = 100
task_timeout = :timer.seconds(10)

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

{:ok, _} = Connection.start_link(name: CONN, pool_size: 50)

Benchee.run(
  %{
    "Writer.write_messages/4" => fn writers ->
      tasks =
        for {stream, messages, count} <- writers do
          Task.async(fn ->
            expected_version = -1 + count
            {:ok, ^expected_version} = Writer.write_messages(CONN, stream, messages, -1)
          end)
        end

      Enum.each(tasks, &Task.await(&1, task_timeout))
    end
  },
  inputs: [
    {"1 writer, #{batch_size} messages", 1},
    {"10 concurrent writers, #{batch_size} messages", 10},
    {"20 concurrent writers, #{batch_size} messages", 20},
    {"50 concurrent writers, #{batch_size} messages", 50}
  ],
  before_each: fn writers_count ->
    for idx <- 1..writers_count do
      {"benchWriterConcurrency-#{UUID.generate()}_#{idx}", Enum.take(message_stream, batch_size),
       batch_size}
    end
  end,
  time: 10
)
