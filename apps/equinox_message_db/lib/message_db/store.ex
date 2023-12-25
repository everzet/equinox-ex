defmodule Equinox.MessageDb.Store do
  alias Equinox.MessageDb.{Reader, Writer}
  alias Equinox.State

  defmodule Unoptimized do
    defmacro __using__(opts) do
      fetch_conn = Keyword.fetch!(opts, :fetch_conn)
      write_conn = Keyword.fetch!(opts, :write_conn)
      batch_size = Keyword.get(opts, :batch_size, 500)

      quote do
        @behaviour Equinox.Store
        @batch_size unquote(batch_size)
        alias Equinox.MessageDb.Store

        def load!(stream, state, codec, fold) do
          Store.load_unoptimized!(unquote(fetch_conn), stream, state, codec, fold, @batch_size)
        end

        def sync!(stream, state, events, ctx, codec, fold) do
          Store.sync!(unquote(write_conn), stream, state, events, ctx, codec, fold)
        end
      end
    end
  end

  defmodule LatestKnownEvent do
    defmacro __using__(opts) do
      fetch_conn = Keyword.fetch!(opts, :fetch_conn)
      write_conn = Keyword.fetch!(opts, :write_conn)

      quote do
        @behaviour Equinox.Store
        alias Equinox.MessageDb.Store

        def load!(stream, state, codec, fold) do
          Store.load_latest_known_event!(unquote(fetch_conn), stream, state, codec, fold)
        end

        def sync!(stream, state, events, ctx, codec, fold) do
          Store.sync!(unquote(write_conn), stream, state, events, ctx, codec, fold)
        end
      end
    end
  end

  def sync!(conn, stream_name, state, events, ctx, codec, fold) do
    State.sync!(state, events, ctx, codec, fold, fn event_data ->
      case Writer.write_messages(conn, stream_name, event_data, state.version) do
        {:ok, new_version} -> new_version
        {:error, exception} -> raise exception
      end
    end)
  end

  def load_unoptimized!(conn, stream_name, state, codec, fold, batch_size) do
    State.load!(state, codec, fold, fn ->
      Reader.stream_stream_messages(conn, stream_name, state.version + 1, batch_size)
    end)
  end

  def load_latest_known_event!(conn, stream_name, state, codec, fold) do
    State.load!(state, codec, fold, fn ->
      case Reader.get_last_stream_message(conn, String.Chars.to_string(stream_name)) do
        {:ok, nil} -> []
        {:ok, message} -> [message]
        {:error, exception} -> raise exception
      end
    end)
  end
end
