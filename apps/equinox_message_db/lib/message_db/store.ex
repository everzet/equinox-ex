defmodule Equinox.MessageDb.Store do
  alias Equinox.MessageDb.{Reader, Writer}
  alias Equinox.Store.{State, Outcome}

  defmodule Unoptimized do
    defmacro __using__(opts) do
      fetch_conn = Keyword.fetch!(opts, :fetch_conn)
      write_conn = Keyword.fetch!(opts, :write_conn)
      batch_size = Keyword.get(opts, :batch_size, 500)

      quote do
        @behaviour Equinox.Store
        @batch_size unquote(batch_size)
        alias Equinox.MessageDb.Store

        def load(stream, state, codec, fold) do
          Store.load_unoptimized(unquote(fetch_conn), stream, state, codec, fold, @batch_size)
        end

        def sync(stream, state, outcome, codec, fold) do
          Store.sync(unquote(write_conn), stream, state, outcome, codec, fold)
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

        def load(stream, state, codec, fold) do
          Store.load_latest_known_event(unquote(fetch_conn), stream, state, codec, fold)
        end

        def sync(stream, state, outcome, codec, fold) do
          Store.sync(unquote(write_conn), stream, state, outcome, codec, fold)
        end
      end
    end
  end

  def sync(conn, stream_name, state, outcome, codec, fold) do
    messages = Outcome.encode(outcome, codec)

    case Writer.write_messages(conn, stream_name, messages, state.version) do
      {:ok, new_version} ->
        new_state =
          outcome.events
          |> fold.fold(state.value)
          |> State.new(new_version)

        {:ok, new_state}

      {:error, error} ->
        {:error, error}
    end
  end

  def load_unoptimized(conn, stream_name, state, codec, fold, batch_size) do
    state = State.ensure_initialized(state, fold, -1)

    conn
    |> Reader.stream_messages(stream_name, state.version + 1, batch_size)
    |> Enum.reduce_while({:ok, state}, fn
      {:ok, timeline_event}, {:ok, state} ->
        new_state =
          timeline_event
          |> codec.decode()
          |> then(&fold.fold([&1], state.value))
          |> State.new(timeline_event.position)

        {:cont, {:ok, new_state}}

      {:error, error}, {:ok, partial_state} ->
        {:halt, {:error, error, partial_state}}
    end)
  end

  def load_latest_known_event(conn, stream_name, state, codec, fold) do
    state = State.ensure_initialized(state, fold, -1)

    case Reader.get_last_stream_message(conn, stream_name) do
      {:ok, nil} ->
        {:ok, state}

      {:ok, timeline_event} ->
        new_state =
          timeline_event
          |> codec.decode()
          |> then(&fold.fold([&1], state.value))
          |> State.new(timeline_event.position)

        {:ok, new_state}

      {:error, error} ->
        {:error, error, state}
    end
  end
end
