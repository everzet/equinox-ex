defmodule Equinox.Events do
  defmodule DomainEvent do
    @type t :: any()
  end

  defmodule EventData do
    @enforce_keys [:id, :type]
    defstruct [:id, :type, :data, :metadata]

    @type t :: %__MODULE__{
            id: String.t(),
            type: String.t(),
            data: map() | nil,
            metadata: map() | nil
          }

    def new(values) when is_list(values) do
      values = Keyword.put_new(values, :id, Equinox.UUID.generate())
      struct!(__MODULE__, values)
    end
  end

  defmodule TimelineEvent do
    @enforce_keys [:id, :type, :stream_name, :position, :global_position, :data, :metadata, :time]
    defstruct [:id, :type, :stream_name, :position, :global_position, :data, :metadata, :time]

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

    def new(values) when is_list(values) do
      struct!(__MODULE__, values)
    end

    def from_data(stream_name, %EventData{} = data, position) do
      new(
        id: data.id,
        type: data.type,
        stream_name: String.Chars.to_string(stream_name),
        position: position,
        global_position: position,
        data: data.data,
        metadata: data.metadata,
        time: NaiveDateTime.utc_now()
      )
    end
  end
end
