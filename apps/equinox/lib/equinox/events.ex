defmodule Equinox.Events do
  defmodule DomainEvent do
    @type t :: any()
    @type with_position :: {t(), non_neg_integer()}
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
  end
end
