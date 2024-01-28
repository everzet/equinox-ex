defmodule Equinox.MessageDb.Store do
  alias Equinox.{Store.State, Store.EventsToSync}
  alias Equinox.{Codec, Codec.StreamName, Fold}
  alias Equinox.Events.TimelineEvent
  alias Equinox.MessageDb.{Reader, Writer}

  @type batch_size :: pos_integer()

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

  defp encode_events(events, codec),
    # We simply convert our domain events into timeline events, but leave data and metadata
    # not serialized.
    # We let Postgrex (through Writer) do its own thing serializing messages on write. It
    # is very optimal at that as it uses IOLists behind the scene. That results in best
    # performance and memory consumption as IOListst are handled very efficiently by VM.
    do: EventsToSync.to_messages(events, codec)

  defp decode_event({:error, error}, _codec), do: {:error, error}
  defp decode_event({:ok, nil}, _codec), do: {:ok, nil}

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

  defp decode_events(events, codec) do
    events
    |> Task.async_stream(&decode_event(&1, codec), ordered: true)
    |> Stream.map(fn {:ok, return} -> return end)
  end

  defp fold_write({:error, error}, _domain_events, _state, _fold), do: {:error, error}

  defp fold_write({:ok, pos}, domain_events, state, fold) do
    {:ok,
     domain_events
     |> fold.fold(state.value)
     |> State.new(pos)}
  end

  defp fold_event(event, {:ok, state}, fold), do: fold_event(event, state, fold)
  defp fold_event({:error, error}, _state, _fold), do: {:error, error}
  defp fold_event({:ok, nil}, state, _fold), do: {:ok, state}

  defp fold_event({:ok, {pos, evt}}, state, fold) do
    {:ok,
     [evt]
     |> fold.fold(state.value)
     |> State.new(pos)}
  end

  defp fold_events(events, state, fold) do
    Enum.reduce_while(events, {:ok, state}, fn
      {:ok, event}, state -> {:cont, fold_event({:ok, event}, state, fold)}
      {:error, error}, {:ok, _partial_state} -> {:halt, {:error, error}}
    end)
  end
end
