defmodule Equinox.MessageDb.Store do
  alias Equinox.MessageDb.{Reader, Writer}
  alias Equinox.Store.{State, EventsToSync}

  def sync(conn, stream_name, state, to_sync, codec, fold) do
    messages = EventsToSync.to_messages(to_sync, codec)

    case Writer.write_messages(conn, stream_name.combined, messages, state.version) do
      {:ok, new_version} ->
        new_state =
          to_sync.events
          |> fold.fold(state.value)
          |> State.new(new_version)

        {:ok, new_state}

      {:error, error} ->
        {:error, error}
    end
  end

  def load_unoptimized(conn, stream_name, state, codec, fold, batch_size) do
    conn
    |> Reader.stream_messages(stream_name.combined, state.version + 1, batch_size)
    |> Enum.reduce_while({:ok, state}, fn
      {:ok, timeline_event}, {:ok, state} ->
        case codec.decode(timeline_event) do
          nil ->
            {:cont, {:ok, state}}

          evt ->
            new_state =
              [evt]
              |> fold.fold(state.value)
              |> State.new(timeline_event.position)

            {:cont, {:ok, new_state}}
        end

      {:error, error}, {:ok, _partial_state} ->
        {:halt, {:error, error}}
    end)
  end

  def load_latest_known_event(conn, stream_name, state, codec, fold) do
    case Reader.get_last_stream_message(conn, stream_name.combined) do
      {:ok, nil} ->
        {:ok, state}

      {:ok, timeline_event} ->
        case codec.decode(timeline_event) do
          nil ->
            {:ok, state}

          evt ->
            new_state =
              [evt]
              |> fold.fold(state.value)
              |> State.new(timeline_event.position)

            {:ok, new_state}
        end

      {:error, error} ->
        {:error, error}
    end
  end
end
