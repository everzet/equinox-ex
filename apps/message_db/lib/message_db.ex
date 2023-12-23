defmodule MessageDb do
  defmacro __using__(opts) do
    postgrex_opts = opts |> Keyword.fetch!(:url) |> MessageDb.UrlParser.parse_url()
    batch_size = opts |> Keyword.get(:batch_size, 500)
    conn_name = unquote(__MODULE__)

    quote do
      alias Equinox.Stream.StreamName

      def child_spec(_) do
        %{
          id: __MODULE__,
          start:
            {MessageDb.Connection, :start_link,
             [Keyword.put(unquote(postgrex_opts), :name, unquote(conn_name))]}
        }
      end

      defmodule Unoptimized do
        @behaviour Equinox.Store

        def fetch_timeline_events(%StreamName{} = stream_name, from_version) do
          MessageDb.Reader.stream_stream_messages(
            unquote(conn_name),
            String.Chars.to_string(stream_name),
            from_version,
            unquote(batch_size)
          )
        end

        def write_event_data(%StreamName{} = stream_name, event_data, expected_version) do
          MessageDb.Writer.write_messages(
            unquote(conn_name),
            String.Chars.to_string(stream_name),
            event_data,
            expected_version
          )
        end
      end

      defmodule LatestKnownEvent do
        @behaviour Equinox.Store

        def fetch_timeline_events(%StreamName{} = stream_name, _from_version) do
          [
            MessageDb.Reader.get_last_stream_message(
              unquote(conn_name),
              String.Chars.to_string(stream_name)
            )
          ]
        end

        def write_event_data(%StreamName{} = stream_name, event_data, expected_version) do
          MessageDb.Writer.write_messages(
            unquote(conn_name),
            String.Chars.to_string(stream_name),
            event_data,
            expected_version
          )
        end
      end
    end
  end
end
