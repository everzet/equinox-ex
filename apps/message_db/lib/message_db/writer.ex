defmodule MessageDb.Writer do
  @type stream_name :: String.t()
  @type expected_version :: -1 | non_neg_integer()
  @type written_position :: non_neg_integer()

  defmodule Message do
    @enforce_keys [:id, :type]
    defstruct [:id, :type, :data, :metadata]

    @type t :: %__MODULE__{
            id: String.t(),
            type: String.t(),
            data: map() | nil,
            metadata: map() | nil
          }

    def new(values) when is_list(values) do
      values = Keyword.put_new(values, :id, MessageDb.UUID.generate())
      struct!(__MODULE__, values)
    end
  end

  defmodule DuplicateMessageId do
    defexception message: "Message with given ID already exists"
    @type t :: %__MODULE__{}
  end

  defmodule StreamVersionConflict do
    defexception message: "Wrong expected version"
    @type t :: %__MODULE__{}
  end

  @spec write_messages(
          Postgrex.conn(),
          stream_name(),
          nonempty_list(Message.t()),
          expected_version()
        ) ::
          {:ok, new_version :: written_position()}
          | {:error, DuplicateMessageId.t() | StreamVersionConflict.t() | Postgrex.Error.t()}
  def write_messages(conn, stream, messages, version) when length(messages) > 0 do
    try do
      Postgrex.transaction(conn, fn conn ->
        query = Postgrex.prepare!(conn, "write", "SELECT write_message($1, $2, $3, $4, $5, $6)")

        %Postgrex.Result{rows: [[written_position]]} =
          messages
          |> Stream.with_index()
          |> Stream.map(fn {m, i} -> [m.id, stream, m.type, m.data, m.metadata, version + i] end)
          |> Stream.map(&Postgrex.execute!(conn, query, &1))
          |> Enum.to_list()
          |> List.last()

        written_position
      end)
    rescue
      error in [Postgrex.Error] ->
        cond do
          error.postgres && error.postgres.message =~ "constraint \"messages_id\"" ->
            {:error, %DuplicateMessageId{}}

          error.postgres && error.postgres.message =~ "Wrong expected version" ->
            {:error, %StreamVersionConflict{}}

          true ->
            {:error, error}
        end
    end
  end
end
