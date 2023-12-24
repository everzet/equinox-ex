defmodule Equinox.MessageDb.Connection do
  alias Equinox.MessageDb.UrlParser

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
    opts =
      case Keyword.get(opts, :url) do
        nil -> opts
        url -> url |> UrlParser.parse_url() |> Keyword.merge(opts)
      end

    Keyword.put_new(opts, :after_connect, fn conn ->
      if Keyword.get(opts, :sql_condition, false) do
        Postgrex.query!(conn, "SET message_store.sql_condition TO on;", [])
      end
    end)
  end
end
