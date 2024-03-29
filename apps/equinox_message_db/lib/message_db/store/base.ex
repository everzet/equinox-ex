defmodule Equinox.MessageDb.Store.Base do
  alias Equinox.{Store.State, Store.EventsToSync}
  alias Equinox.{Codec, Codec.StreamName, Fold}
  alias Equinox.Events.{EventData, TimelineEvent}
  alias Equinox.MessageDb.{Reader, Writer}

  @type batch_size :: pos_integer()

  @serializer Jason
  @decode_timeout :timer.seconds(5)

  @spec sync(Postgrex.conn(), StreamName.t(), State.t(), EventsToSync.t(), Codec.t(), Fold.t()) ::
          {:ok, State.t()}
          | {:error, Exception.t()}
  def sync(conn, stream_name, state, events, codec, fold) do
    events
    |> encode_events(codec)
    |> then(&Writer.write_messages(conn, stream_name.whole, &1, state.version))
    |> fold_write(events.events, state, fold)
  end

  @spec load_unoptimized(
          Postgrex.conn(),
          StreamName.t(),
          State.t(),
          Codec.t(),
          Fold.t(),
          batch_size()
        ) ::
          {:ok, State.t()}
          | {:error, Exception.t()}
  def load_unoptimized(conn, stream_name, state, codec, fold, batch_size) do
    conn
    |> Reader.stream_messages(stream_name.whole, state.version + 1, batch_size)
    |> decode_events(codec)
    |> fold_events(state, fold)
  end

  @spec load_latest_known_event(Postgrex.conn(), StreamName.t(), State.t(), Codec.t(), Fold.t()) ::
          {:ok, State.t()}
          | {:error, Exception.t()}
  def load_latest_known_event(conn, stream_name, state, codec, fold) do
    conn
    |> Reader.get_last_stream_message(stream_name.whole)
    |> decode_event(codec)
    |> fold_event(state, fold)
  end

  defp encode_events(events, codec) do
    Enum.map(events.events, fn event ->
      event
      |> codec.encode(events.context)
      |> EventData.update_data(&serialize_data/1)
      |> EventData.update_metadata(&serialize_data/1)
    end)
  end

  defp decode_event({:error, error}, _codec), do: {:error, error}
  defp decode_event({:ok, event}, codec), do: decode_event(event, codec)
  defp decode_event(nil, _codec), do: {:ok, nil}

  defp decode_event(event, codec) do
    {:ok,
     {event.position,
      event
      |> TimelineEvent.update_data(&deserialize_data/1)
      |> TimelineEvent.update_metadata(&deserialize_data/1)
      |> codec.decode()}}
  end

  defp decode_events(events, codec) do
    events
    |> Task.async_stream(&decode_event(&1, codec), ordered: true, timeout: @decode_timeout)
    |> Stream.map(fn {:ok, task_return} -> task_return end)
  end

  defp fold_write({:error, error}, _domain_events, _state, _fold), do: {:error, error}
  defp fold_write({:ok, pos}, events, state, fold), do: fold_event({pos, events}, state, fold)

  defp fold_event({:error, error}, _state, _fold), do: {:error, error}
  defp fold_event({:ok, event}, state, fold), do: fold_event(event, state, fold)
  defp fold_event(event, {:ok, state}, fold), do: fold_event(event, state, fold)
  defp fold_event(nil, state, _fold), do: {:ok, state}

  defp fold_event({position, event}, state, fold) do
    {:ok,
     event
     |> List.wrap()
     |> fold.fold(state.value)
     |> State.new(position)}
  end

  defp fold_events(events, state, fold) do
    Enum.reduce_while(events, {:ok, state}, fn
      {:error, error}, {:ok, _partial_state} -> {:halt, {:error, error}}
      {:ok, event}, {:ok, state} -> {:cont, fold_event(event, state, fold)}
    end)
  end

  defp deserialize_data(str) when is_bitstring(str), do: @serializer.decode!(str)
  defp deserialize_data(anything_else), do: anything_else

  # We do not serialize event data or metadata ourselves.
  # Instead we rely on Postgrex (through Writer) to do its own thing and serialize
  # messages on write for us.
  #
  # Postgrex uses IOLists behind the scene, which results in best performance and
  # memory consumption. Better than anything we would be able to produce on our own.
  defp serialize_data(unserialized_value), do: unserialized_value
end
