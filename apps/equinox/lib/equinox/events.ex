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
            data: term() | binary() | nil,
            metadata: term() | binary() | nil
          }

    @spec new(keyword()) :: t()
    def new(values) when is_list(values) do
      values
      |> Keyword.put_new(:id, Equinox.UUID.generate())
      |> then(&struct!(__MODULE__, &1))
    end

    @spec update_data(t(), (term() | binary() | nil -> term() | binary() | nil)) :: t()
    def update_data(%__MODULE__{} = event, fun), do: %{event | data: fun.(event.data)}

    @spec update_metadata(t(), (term() | binary() | nil -> term() | binary() | nil)) :: t()
    def update_metadata(%__MODULE__{} = event, fun), do: %{event | metadata: fun.(event.metadata)}
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
            data: term() | binary() | nil,
            metadata: term() | binary() | nil,
            time: NaiveDateTime.t()
          }

    @spec new(keyword()) :: t()
    def new(values) when is_list(values), do: struct!(__MODULE__, values)

    @spec update_data(t(), (term() | binary() | nil -> term() | binary() | nil)) :: t()
    def update_data(%__MODULE__{} = event, fun), do: %{event | data: fun.(event.data)}

    @spec update_metadata(t(), (term() | binary() | nil -> term() | binary() | nil)) :: t()
    def update_metadata(%__MODULE__{} = event, fun), do: %{event | metadata: fun.(event.metadata)}
  end
end
