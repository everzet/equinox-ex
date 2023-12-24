defmodule Equinox.MessageDb.Reader do
  alias Equinox.Events.TimelineEvent

  @type category_name :: String.t()
  @type stream_name :: String.t()
  @type position :: non_neg_integer()
  @type global_position :: non_neg_integer()
  @type batch_size :: pos_integer()
  @type consumer_group :: {member :: non_neg_integer(), size :: pos_integer()}

  @spec get_category_messages(
          Postgrex.conn(),
          category_name(),
          global_position(),
          batch_size(),
          consumer_group() | {nil, nil}
        ) :: {:ok, list(TimelineEvent.t())} | {:error, Exception.t()}
  def get_category_messages(conn, category, position, batch_size, {member, size} \\ {nil, nil}) do
    conn
    |> Postgrex.query(
      "SELECT id, type, stream_name, position, global_position, data::jsonb, metadata::jsonb, time
       FROM get_category_messages($1, $2, $3, null, $4, $5)",
      [category, position, batch_size, member, size],
      decode_mapper: fn row ->
        ~w(id type stream_name position global_position data metadata time)a
        |> Enum.zip(row)
        |> TimelineEvent.new()
      end
    )
    |> case do
      {:ok, result} -> {:ok, List.wrap(result.rows)}
      anything_else -> anything_else
    end
  end

  @spec get_last_stream_message(Postgrex.conn(), stream_name()) ::
          {:ok, TimelineEvent.t() | nil} | {:error, Exception.t()}
  def get_last_stream_message(conn, stream) do
    conn
    |> Postgrex.query(
      "SELECT id, type, stream_name, position, global_position, data::jsonb, metadata::jsonb, time
       FROM get_last_stream_message($1)",
      [stream],
      decode_mapper: fn row ->
        ~w(id type stream_name position global_position data metadata time)a
        |> Enum.zip(row)
        |> TimelineEvent.new()
      end
    )
    |> case do
      {:ok, result} -> {:ok, result.rows |> List.wrap() |> List.first()}
      anything_else -> anything_else
    end
  end

  @spec get_stream_messages(Postgrex.conn(), stream_name(), position(), batch_size()) ::
          {:ok, list(TimelineEvent.t())} | {:error, Exception.t()}
  def get_stream_messages(conn, stream, position, batch_size) do
    conn
    |> Postgrex.query(
      "SELECT id, type, stream_name, position, global_position, data::jsonb, metadata::jsonb, time
       FROM get_stream_messages($1, $2, $3)",
      [stream, position, batch_size],
      decode_mapper: fn row ->
        ~w(id type stream_name position global_position data metadata time)a
        |> Enum.zip(row)
        |> TimelineEvent.new()
      end
    )
    |> case do
      {:ok, result} -> {:ok, List.wrap(result.rows)}
      anything_else -> anything_else
    end
  end

  @spec stream_stream_messages(Postgrex.conn(), stream_name(), position(), batch_size()) ::
          Enumerable.t(TimelineEvent.t())
  def stream_stream_messages(conn, stream, start_position, batch_size) do
    {start_position, batch_size}
    |> Stream.unfold(fn {position, batch_size} ->
      case get_stream_messages(conn, stream, position, batch_size) do
        {:ok, []} -> nil
        {:ok, messages} -> {messages, {position + length(messages), batch_size}}
        {:error, error} -> raise error
      end
    end)
    |> Stream.flat_map(& &1)
  end
end
