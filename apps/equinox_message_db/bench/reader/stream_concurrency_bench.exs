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
batch_size = 100
task_timeout = :timer.seconds(10)

{:ok, _} = Connection.start_link(name: CONN, pool_size: 50)
{:ok, _} = Writer.write_messages(CONN, stream, Enum.take(message_stream, 100), -1)

Benchee.run(
  %{
    "Reader.get_stream_messages/4" => fn readers ->
      tasks =
        for _ <- 1..readers do
          Task.async(fn ->
            Reader.stream_messages(CONN, stream, 0, 500) |> Enum.to_list()
          end)
        end

      Enum.each(tasks, &Task.await(&1, task_timeout))
    end
  },
  inputs: [
    {"single reader, #{batch_size} messages", 1},
    {"10 concurrent readers, #{batch_size} messages", 10},
    {"20 concurrent readers, #{batch_size} messages", 20},
    {"50 concurrent readers, #{batch_size} messages", 50}
  ],
  time: 10
)
