defmodule Equinox.MessageDb.Writer do
  alias Equinox.Events.EventData
  alias Equinox.Store.Errors

  @type stream_name :: String.t()
  @type expected_version :: -1 | non_neg_integer()
  @type written_position :: non_neg_integer()

  @spec write_messages(Postgrex.conn(), stream_name(), list(EventData.t()), expected_version()) ::
          {:ok, new_version :: written_position()} | {:error, Errors.t() | Postgrex.Error.t()}
  def write_messages(conn, stream, messages, version) do
    conn
    |> do_write_messages(stream, messages, version)
    |> handle_write_result()
  end

  defp do_write_messages(_conn, _stream, [], version), do: {:ok, version}

  defp do_write_messages(conn, stream, [message], version) do
    with {:ok, _query, result} <- write_single_message(conn, stream, message, version) do
      {:ok, result}
    end
  end

  defp do_write_messages(conn, stream, [first | rest], version) do
    Postgrex.transaction(conn, fn conn ->
      with {:ok, query, _res} <- write_single_message(conn, stream, first, version),
           {:ok, last_result} <- write_multiple_messages(conn, query, stream, rest, version + 1) do
        last_result
      else
        {:error, error} -> Postgrex.rollback(conn, error)
      end
    end)
  end

  defp handle_write_result({:ok, position}) when is_number(position), do: {:ok, position}

  defp handle_write_result({:ok, %Postgrex.Result{rows: [[written_position]]}}) do
    {:ok, written_position}
  end

  defp handle_write_result({:error, %Postgrex.Error{postgres: postgres} = error}) do
    cond do
      is_map(postgres) and postgres.message =~ "Wrong expected version" ->
        {stream_name, stream_version} =
          case capture_stream_name_and_version(postgres.message) do
            %{"stream" => stream, "version" => version} -> {stream, String.to_integer(version)}
            nil -> {nil, nil}
          end

        exception = %Errors.StreamVersionConflict{
          message: postgres.message,
          stream_name: stream_name,
          stream_version: stream_version
        }

        {:error, exception}

      is_map(postgres) and postgres.message =~ "constraint \"messages_id\"" ->
        message_id =
          case capture_duplicate_id(postgres.detail || "") do
            %{"id" => id} -> id
            nil -> nil
          end

        exception = %Errors.DuplicateMessageId{message: postgres.message, message_id: message_id}
        {:error, exception}

      true ->
        {:error, error}
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

  defp capture_stream_name_and_version(message) do
    Regex.named_captures(
      ~r/\(Stream: (?<stream>[^,]+), Stream Version: (?<version>.+)\)/i,
      message
    )
  end

  defp capture_duplicate_id(detail) do
    Regex.named_captures(~r/Key \(id\)=\((?<id>.*)\) already exists\./i, detail)
  end
end
