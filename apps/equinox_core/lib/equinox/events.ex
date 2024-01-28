defmodule Equinox.Events do
  defmodule DomainEvent do
    @type t :: term()
  end

  defmodule EventData do
    @enforce_keys [:id, :type]
    defstruct [:id, :type, :data, :metadata]

    @type t :: %__MODULE__{
            id: String.t(),
            type: String.t(),
            data: map() | binary() | nil,
            metadata: map() | binary() | nil
          }

    def new(values) when is_list(values) do
      values = Keyword.put_new(values, :id, Equinox.UUID.generate())
      struct!(__MODULE__, values)
    end

    def update_data(%__MODULE__{} = e, fun), do: %{e | data: fun.(e.data)}
    def update_metadata(%__MODULE__{} = e, fun), do: %{e | metadata: fun.(e.metadata)}
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
            data: map() | binary() | nil,
            metadata: map() | binary() | nil,
            time: NaiveDateTime.t()
          }

    def new(values) when is_list(values) do
      struct!(__MODULE__, values)
    end

    def update_data(%__MODULE__{} = e, fun), do: %{e | data: fun.(e.data)}
    def update_metadata(%__MODULE__{} = e, fun), do: %{e | metadata: fun.(e.metadata)}
  end
end
