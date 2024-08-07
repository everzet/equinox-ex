defmodule Equinox.MessageDb.Reader do
  alias Equinox.Events.TimelineEvent

  @type stream_position :: non_neg_integer()
  @type global_position :: non_neg_integer()
  @type batch_size :: pos_integer()
  @type consumer_group :: {member :: non_neg_integer(), size :: pos_integer()} | {nil, nil}

  @spec get_category_messages(
          Postgrex.conn(),
          String.t(),
          global_position(),
          batch_size(),
          consumer_group()
        ) :: {:ok, list(TimelineEvent.t())} | {:error, Exception.t()}
  def get_category_messages(
        conn,
        category,
        position,
        batch_size,
        {group_member, group_size} \\ {nil, nil}
      ) do
    conn
    |> Postgrex.query(
      "SELECT id, type, stream_name, position, global_position, data::text, metadata::text, time
       FROM get_category_messages($1, $2, $3, null, $4, $5)",
      [category, position, batch_size, group_member, group_size],
      decode_mapper: fn row ->
        # the order matters here and must match the order of SELECT in the query above:
        ~w(id type stream_name position global_position data metadata time)a
        |> Enum.zip(row)
        |> TimelineEvent.new()
      end
    )
    |> list_wrap_results()
  end

  @spec get_stream_messages(Postgrex.conn(), String.t(), stream_position(), batch_size()) ::
          {:ok, list(TimelineEvent.t())} | {:error, Exception.t()}
  def get_stream_messages(conn, stream, position, batch_size) do
    conn
    |> Postgrex.query(
      "SELECT id, type, stream_name, position, global_position, data::text, metadata::text, time
       FROM get_stream_messages($1, $2, $3)",
      [stream, position, batch_size],
      decode_mapper: fn row ->
        # the order matters here and must match the order of SELECT in the query above:
        ~w(id type stream_name position global_position data metadata time)a
        |> Enum.zip(row)
        |> TimelineEvent.new()
      end
    )
    |> list_wrap_results()
  end

  @spec get_last_stream_message(Postgrex.conn(), String.t()) ::
          {:ok, TimelineEvent.t() | nil} | {:error, Exception.t()}
  def get_last_stream_message(conn, stream) do
    conn
    |> Postgrex.query(
      "SELECT id, type, stream_name, position, global_position, data::text, metadata::text, time
       FROM get_last_stream_message($1)",
      [stream],
      decode_mapper: fn row ->
        # the order matters here and must match the order of SELECT in the query above:
        ~w(id type stream_name position global_position data metadata time)a
        |> Enum.zip(row)
        |> TimelineEvent.new()
      end
    )
    |> list_wrap_results()
    |> get_first_result()
  end

  @spec stream_messages(Postgrex.conn(), String.t(), stream_position(), batch_size()) ::
          Enumerable.t({:ok, TimelineEvent.t()} | {:error, Exception.t()})
  def stream_messages(conn, stream, start_position, batch_size) do
    {start_position, batch_size}
    |> Stream.unfold(fn
      {position, batch_size} ->
        case get_stream_messages(conn, stream, position, batch_size) do
          {:ok, []} ->
            nil

          {:ok, messages} ->
            okayed_messages = Enum.map(messages, &{:ok, &1})
            next_batch = {position + length(messages), batch_size}
            {okayed_messages, next_batch}

          {:error, error} ->
            {[{:error, error}], nil}
        end

      nil ->
        nil
    end)
    |> Stream.flat_map(& &1)
  end

  defp list_wrap_results({:ok, result}), do: {:ok, List.wrap(result.rows)}
  defp list_wrap_results(anything_else), do: anything_else

  defp get_first_result({:ok, results}), do: {:ok, List.first(results)}
  defp get_first_result(anything_else), do: anything_else
end
