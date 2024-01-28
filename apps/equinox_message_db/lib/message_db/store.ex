defmodule Equinox.MessageDb.Store do
  alias Equinox.Events.TimelineEvent
  alias Equinox.MessageDb.{Reader, Writer}
  alias Equinox.Store.{State, EventsToSync}

  def sync(conn, stream_name, state, events, codec, fold) do
    events
    |> EventsToSync.to_messages(codec)
    |> encode_messages()
    |> then(&Writer.write_messages(conn, stream_name.whole, &1, state.version))
    |> fold_write(events.events, state, fold)
  end

  def load_unoptimized(conn, stream_name, state, codec, fold, batch_size) do
    conn
    |> Reader.stream_messages(stream_name.whole, state.version + 1, batch_size)
    |> decode_event_stream(codec)
    |> fold_event_stream({:ok, state}, fold)
  end

  def load_latest_known_event(conn, stream_name, state, codec, fold) do
    conn
    |> Reader.get_last_stream_message(stream_name.whole)
    |> decode_event(codec)
    |> fold_event({:ok, state}, fold)
  end

  defp encode_messages(messages),
    # We let Postgrex do its thing encoding messages _because it is much more optimal
    # than anything else we would be able to come up with here. It uses IOLists.
    do: messages

  defp fold_write({:error, error}, _domain_events, _state, _fold), do: {:error, error}

  defp fold_write({:ok, pos}, domain_events, state, fold) do
    {:ok,
     domain_events
     |> fold.fold(state.value)
     |> State.new(pos)}
  end

  defp decode_event({:error, error}, _codec), do: {:error, error}
  defp decode_event({:ok, nil}, _codec), do: {:ok, {-1, nil}}

  defp decode_event({:ok, timeline_event}, codec) do
    {:ok,
     {timeline_event.position,
      timeline_event
      |> TimelineEvent.update_data(&decode_data!/1)
      |> TimelineEvent.update_metadata(&decode_data!/1)
      |> codec.decode()}}
  end

  defp decode_data!(str) when is_bitstring(str), do: Jason.decode!(str)
  defp decode_data!(anything_else), do: anything_else

  defp decode_event_stream(event_stream, codec) do
    event_stream
    |> Task.async_stream(&decode_event(&1, codec), ordered: true)
    |> Stream.map(fn
      {:ok, result} -> result
      {:exit, reason} -> raise RuntimeError, "Failed to decode event: #{inspect(reason)}"
    end)
  end

  defp fold_event({:error, error}, {:ok, _state}, _fold), do: {:error, error}
  defp fold_event({:ok, {_pos, nil}}, {:ok, state}, _fold), do: {:ok, state}

  defp fold_event({:ok, {pos, evt}}, {:ok, state}, fold) do
    {:ok,
     [evt]
     |> fold.fold(state.value)
     |> State.new(pos)}
  end

  defp fold_event_stream(event_stream, state, fold) do
    Enum.reduce_while(event_stream, state, fn
      {:ok, event}, state -> {:cont, fold_event({:ok, event}, state, fold)}
      {:error, error}, {:ok, _partial_state} -> {:halt, {:error, error}}
    end)
  end
end
