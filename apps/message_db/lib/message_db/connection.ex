defmodule MessageDb.Connection do
  def start_link(opts \\ []) do
    opts
    |> default_opts()
    |> Postgrex.start_link()
  end

  def child_spec(opts) do
    opts
    |> default_opts()
    |> Postgrex.child_spec()
  end

  defp default_opts(opts) do
    opts
    |> Keyword.put_new(:after_connect, fn conn ->
      if Keyword.get(opts, :sql_condition, false) do
        Postgrex.query!(conn, "SET message_store.sql_condition TO on;", [])
      end
    end)
  end
end
