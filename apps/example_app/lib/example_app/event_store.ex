defmodule ExampleApp.EventStore do
  defmodule Unoptimized do
    use Equinox.MessageDb.Store.Unoptimized,
      fetch_conn: ExampleApp.MessageDb,
      write_conn: ExampleApp.MessageDb,
      batch_size: 500
  end

  defmodule LatestKnownEvent do
    use Equinox.MessageDb.Store.LatestKnownEvent,
      fetch_conn: ExampleApp.MessageDb,
      write_conn: ExampleApp.MessageDb
  end
end
