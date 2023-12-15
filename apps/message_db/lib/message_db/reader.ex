defmodule MessageDb.Reader do
  @type stream_name :: String.t()
  @type position :: non_neg_integer()
  @type batch_size :: pos_integer()

  defmodule Message do
    @enforce_keys [:id, :type, :position, :global_position, :data, :metadata, :time]
    defstruct [:id, :type, :position, :global_position, :data, :metadata, :time]

    @type t :: %__MODULE__{
            id: String.t(),
            type: String.t(),
            position: non_neg_integer(),
            global_position: non_neg_integer(),
            data: map() | nil,
            metadata: map() | nil,
            time: NaiveDateTime.t()
          }

    def from_message_db(row) do
      struct!(__MODULE__,
        id: row["id"],
        type: row["type"],
        position: row["position"],
        global_position: row["global_position"],
        data: maybe_decode_jsonb(row["data"]),
        metadata: maybe_decode_jsonb(row["metadata"]),
        time: row["time"]
      )
    end

    defp maybe_decode_jsonb(col) when is_bitstring(col), do: Jason.decode!(col)
    defp maybe_decode_jsonb(col), do: col
  end

  @spec get_last_stream_message(Postgrex.conn(), stream_name()) ::
          {:ok, Message.t() | nil} | {:error, Postgrex.Error.t()}
  def get_last_stream_message(conn, stream) do
    with {:ok, res} <-
           Postgrex.query(
             conn,
             "SELECT * FROM get_last_stream_message($1)",
             [stream]
           ) do
      message =
        res
        |> messages_from_result()
        |> List.last(nil)

      {:ok, message}
    end
  end

  @spec get_stream_messages(Postgrex.conn(), stream_name(), position(), batch_size()) ::
          {:ok, list(Message.t())} | {:error, Postgrex.Error.t()}
  def get_stream_messages(conn, stream, position, batch_size) do
    with {:ok, res} <-
           Postgrex.query(
             conn,
             "SELECT * FROM get_stream_messages($1, $2, $3)",
             [stream, position, batch_size]
           ) do
      messages = messages_from_result(res)
      {:ok, messages}
    end
  end

  @spec stream_stream_messages(Postgrex.conn(), stream_name(), position(), batch_size()) ::
          Enumerable.t()
  def stream_stream_messages(conn, stream, start_position, batch_size) do
    Stream.resource(
      fn -> {start_position, batch_size} end,
      fn {position, batch_size} ->
        case get_stream_messages(conn, stream, position, batch_size) do
          {:ok, []} -> {:halt, {position, batch_size}}
          {:ok, messages} -> {messages, {position + length(messages), batch_size}}
          {:error, error} -> raise error
        end
      end,
      fn _ -> nil end
    )
  end

  defp messages_from_result(%Postgrex.Result{columns: cols, rows: rows}) do
    rows
    |> Stream.map(&Enum.zip(cols, &1))
    |> Stream.map(&Enum.into(&1, %{}))
    |> Stream.map(&Message.from_message_db/1)
    |> Enum.to_list()
  end
end
