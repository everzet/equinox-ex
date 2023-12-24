defmodule Equinox.MessageDb.ConnectionTest do
  use Equinox.MessageDb.ConnCase

  @tag sql_condition: true
  test_in_isolation "sql_condition can be enabled per connection", %{conn: conn} do
    Postgrex.query!(
      conn,
      "SELECT * FROM get_stream_messages('testStream-42', 0, 1000, condition => 'messages.time >= current_timestamp');",
      []
    )
  end
end
