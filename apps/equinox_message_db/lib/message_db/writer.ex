defmodule Equinox.MessageDb.Writer do
  alias Equinox.Events.EventData
  alias Equinox.Store.{DuplicateMessageId, StreamVersionConflict}

  @type stream_name :: String.t()
  @type expected_version :: -1 | non_neg_integer()
  @type written_position :: non_neg_integer()

  @spec write_messages(Postgrex.conn(), stream_name(), list(EventData.t()), expected_version()) ::
          {:ok, new_version :: written_position()}
          | {:error, DuplicateMessageId.t() | StreamVersionConflict.t() | Postgrex.Error.t()}
  def write_messages(conn, stream, messages, version) do
    messages
    |> case do
      [] ->
        {:ok, version}

      [message] ->
        with {:ok, _query, res} <- write_single_message(conn, stream, message, version) do
          {:ok, res}
        end

      [first | rest] ->
        Postgrex.transaction(conn, fn conn ->
          with {:ok, qry, _res} <- write_single_message(conn, stream, first, version),
               {:ok, last_res} <- write_multiple_messages(conn, qry, stream, rest, version + 1) do
            last_res
          else
            {:error, error} -> Postgrex.rollback(conn, error)
          end
        end)
    end
    |> case do
      {:ok, %Postgrex.Result{rows: [[written_position]]}} ->
        {:ok, written_position}

      {:error, %Postgrex.Error{postgres: postgres} = error} when is_map(postgres) ->
        cond do
          postgres.message =~ "Wrong expected version" ->
            {:error, %StreamVersionConflict{}}

          postgres.message =~ "constraint \"messages_id\"" ->
            {:error, %DuplicateMessageId{}}

          true ->
            {:error, error}
        end

      anything_else ->
        anything_else
    end
  end

  defp write_single_message(conn, stream, message, version) do
    Postgrex.prepare_execute(
      conn,
      "write_message",
      "SELECT write_message($1, $2, $3, $4, $5, $6)",
      [message.id, stream, message.type, message.data, message.metadata, version]
    )
  end

  defp write_multiple_messages(conn, prepared_query, stream, messages, version) do
    messages
    |> Stream.with_index()
    |> Enum.reduce_while(nil, fn {message, idx}, _ ->
      conn
      |> Postgrex.execute(
        prepared_query,
        [message.id, stream, message.type, message.data, message.metadata, version + idx]
      )
      |> case do
        {:ok, _query, res} -> {:cont, {:ok, res}}
        anything_else -> {:halt, anything_else}
      end
    end)
  end
end