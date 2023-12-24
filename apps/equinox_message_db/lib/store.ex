defmodule Equinox.MessageDb.Store do
  alias Equinox.Events.DomainEvent
  alias Equinox.{State, Codec, Fold}
  alias Equinox.MessageDb.{Reader, Writer}

  @callback load!(GenServer.server(), String.t(), State.t(), Codec.t(), Fold.t()) :: State.t()
  @callback sync!(
              GenServer.server(),
              String.t(),
              State.t(),
              list(DomainEvent.t()),
              Codec.ctx(),
              Codec.t(),
              Fold.t()
            ) :: State.t()

  defmodule Unoptimized do
    @behaviour Equinox.MessageDb.Store
    @batch_size 500

    def load!(conn, stream_name, state, codec, fold) do
      State.load!(state, codec, fold, fn ->
        Reader.stream_stream_messages(conn, stream_name, state.version + 1, @batch_size)
      end)
    end

    def sync!(conn, stream_name, state, events, ctx, codec, fold) do
      State.sync!(state, events, ctx, codec, fold, fn event_data ->
        case Writer.write_messages(conn, stream_name, event_data, state.version) do
          {:ok, new_version} -> new_version
          {:error, exception} -> raise exception
        end
      end)
    end
  end

  defmodule LatestKnownEvent do
    @behaviour Equinox.MessageDb.Store

    def load!(conn, stream_name, state, codec, fold) do
      State.load!(state, codec, fold, fn ->
        case Reader.get_last_stream_message(conn, String.Chars.to_string(stream_name)) do
          {:ok, message} -> [message]
          {:error, exception} -> raise exception
        end
      end)
    end

    defdelegate sync!(conn, stream_name, state, events, ctx, codec, fold), to: Unoptimized
  end
end
