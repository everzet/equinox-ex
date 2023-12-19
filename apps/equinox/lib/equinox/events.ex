defmodule Equinox.Events do
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
            stream_name: Reader.stream_name(),
            position: Reader.position(),
            global_position: Reader.global_position(),
            data: map() | nil,
            metadata: map() | nil,
            time: NaiveDateTime.t()
          }

    def new(values) when is_list(values) do
      struct!(__MODULE__, values)
    end
  end

  defmodule Codec do
    defmodule CodecError do
      defexception [:message]
      @type t :: %__MODULE__{message: String.t()}
    end

    @callback encode(domain_event :: any(), context :: any()) ::
                {:ok, EventData.t()} | {:error, CodecError.t()}
    @callback decode(TimelineEvent.t()) ::
                {:ok, domain_event :: any()} | {:error, CodecError.t()}

    defmodule EventStructs do
      defmacro __using__(structs_module: structs_mod) do
        quote do
          @behaviour Equinox.Events.Codec
          @structs_mod unquote(structs_mod)

          @impl Equinox.Events.Codec
          def encode(%{__struct__: struct} = event, _ctx) do
            Equinox.Events.Codec.EventStructs.struct_to_event_data(event, @structs_mod)
          end

          @impl Equinox.Events.Codec
          def decode(event) do
            Equinox.Events.Codec.EventStructs.timeline_event_to_struct(event, @structs_mod)
          end

          defoverridable encode: 2
          defoverridable decode: 1
        end
      end

      def struct_to_event_data(%{__struct__: module} = event, parent_module) do
        parent_type = Atom.to_string(parent_module)
        full_type = Atom.to_string(module)

        if String.starts_with?(full_type, parent_type) do
          type = String.replace_leading(full_type, parent_type <> ".", "")
          data = event |> Map.from_struct() |> Map.new(fn {k, v} -> {Atom.to_string(k), v} end)
          {:ok, Equinox.Events.EventData.new(type: type, data: data)}
        else
          {:error,
           %CodecError{
             message: "Codec.encode: expected struct under #{parent_type}, got #{full_type}"
           }}
        end
      end

      def struct_to_event_data(not_struct, _) do
        {:error,
         %CodecError{message: "Codec.encode: expected struct, got #{inspect(not_struct)}"}}
      end

      def timeline_event_to_struct(%{type: type, data: data}, parent_module) do
        try do
          module = String.to_existing_atom("#{Atom.to_string(parent_module)}.#{type}")
          struct = struct!(module, for({k, v} <- data, do: {String.to_existing_atom(k), v}))
          {:ok, struct}
        rescue
          e in [ArgumentError] ->
            {:error, %CodecError{message: "Codec.decode: #{e.message}"}}
        end
      end
    end
  end
end
