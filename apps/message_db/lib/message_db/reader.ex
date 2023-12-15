defmodule MessageDb.Reader do
  @type category_name :: String.t()
  @type stream_name :: String.t()
  @type position :: non_neg_integer()
  @type batch_size :: pos_integer()
  @type consumer_group :: {member :: non_neg_integer(), size :: pos_integer()}

  defmodule Message do
    @keys [:id, :type, :stream_name, :position, :global_position, :data, :metadata, :time]
    @key_col_map Enum.map(@keys, &{&1, Atom.to_string(&1)})

    @enforce_keys @keys
    defstruct @keys

    @type t :: %__MODULE__{
            id: String.t(),
            type: String.t(),
            stream_name: String.t(),
            position: non_neg_integer(),
            global_position: non_neg_integer(),
            data: map() | nil,
            metadata: map() | nil,
            time: NaiveDateTime.t()
          }

    def from_db(row) when is_map(row) do
      struct!(__MODULE__, Enum.map(@key_col_map, fn {key, col} -> {key, row[col]} end))
    end
  end

  @spec get_category_messages(
          Postgrex.conn(),
          category_name(),
          position(),
          batch_size(),
          consumer_group() | {nil, nil}
        ) :: {:ok, list(Message.t())} | {:error, Postgrex.Error.t()}
  def get_category_messages(conn, category, position, batch_size, {member, size} \\ {nil, nil}) do
    with {:ok, res} <-
           Postgrex.query(
             conn,
             "SELECT
              id, type, stream_name, position, global_position, data::jsonb, metadata::jsonb, time
              FROM get_category_messages($1, $2, $3, null, $4, $5)",
             [category, position, batch_size, member, size]
           ) do
      {:ok, res |> messages_from_result() |> Enum.to_list()}
    end
  end

  @spec get_last_stream_message(Postgrex.conn(), stream_name()) ::
          {:ok, Message.t() | nil} | {:error, Postgrex.Error.t()}
  def get_last_stream_message(conn, stream) do
    with {:ok, res} <-
           Postgrex.query(
             conn,
             "SELECT
              id, type, stream_name, position, global_position, data::jsonb, metadata::jsonb, time
              FROM get_last_stream_message($1)",
             [stream]
           ) do
      {:ok, res |> messages_from_result() |> Enum.at(0)}
    end
  end

  @spec get_stream_messages(Postgrex.conn(), stream_name(), position(), batch_size()) ::
          {:ok, list(Message.t())} | {:error, Postgrex.Error.t()}
  def get_stream_messages(conn, stream, position, batch_size) do
    with {:ok, res} <-
           Postgrex.query(
             conn,
             "SELECT
              id, type, stream_name, position, global_position, data::jsonb, metadata::jsonb, time
              FROM get_stream_messages($1, $2, $3)",
             [stream, position, batch_size]
           ) do
      {:ok, res |> messages_from_result() |> Enum.to_list()}
    end
  end

  @spec stream_stream_messages(Postgrex.conn(), stream_name(), position(), batch_size()) ::
          Enumerable.t(Message.t())
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

  defp messages_from_result(%Postgrex.Result{columns: cols, rows: rows}) do
    rows
    |> Stream.map(&Enum.zip(cols, &1))
    |> Stream.map(&Map.new/1)
    |> Stream.map(&Message.from_db/1)
  end
end
