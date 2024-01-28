defmodule Equinox.Events do
  defmodule DomainEvent do
    alias Equinox.Codec
    alias Equinox.Events.EventData

    @type t :: term()
    @type serialize :: (term() | nil -> term() | binary() | nil)

    @spec encode(t(), Codec.t(), Codec.context(), serialize()) :: EventData.t()
    def encode(event, codec, context, serialize) do
      event
      |> codec.encode(context)
      |> EventData.update_data(&serialize_data(&1, serialize))
      |> EventData.update_metadata(&serialize_data(&1, serialize))
    end

    defp serialize_data(nil, _serialize), do: nil
    defp serialize_data(str, _serialize) when is_bitstring(str), do: str
    defp serialize_data(term, serialize), do: serialize.(term)
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
    alias Equinox.Codec

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
    @type deserialize :: (term() | binary() | nil -> term() | nil)

    @spec new(keyword()) :: t()
    def new(values) when is_list(values), do: struct!(__MODULE__, values)

    @spec update_data(t(), (term() | binary() | nil -> term() | binary() | nil)) :: t()
    def update_data(%__MODULE__{} = event, fun), do: %{event | data: fun.(event.data)}

    @spec update_metadata(t(), (term() | binary() | nil -> term() | binary() | nil)) :: t()
    def update_metadata(%__MODULE__{} = event, fun), do: %{event | metadata: fun.(event.metadata)}

    @spec decode(t(), deserialize(), Codec.t()) :: DomainEvent.t()
    def decode(%__MODULE__{} = event, deserialize, codec) do
      event
      |> update_data(&deserialize_data(&1, deserialize))
      |> update_metadata(&deserialize_data(&1, deserialize))
      |> codec.decode()
    end

    defp deserialize_data(str, deserialize) when is_bitstring(str), do: deserialize.(str)
    defp deserialize_data(anything_else, _deserialize), do: anything_else
  end
end
