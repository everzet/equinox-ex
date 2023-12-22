defmodule Equinox.Codec do
  alias Equinox.Events.{DomainEvent, EventData, TimelineEvent}

  @type t :: module()
  @type context :: any()

  @callback encode(DomainEvent.t(), context()) :: {:ok, EventData.t()} | {:error, CodecError.t()}
  @callback decode(TimelineEvent.t()) :: {:ok, DomainEvent.t()} | {:error, CodecError.t()}

  defmodule CodecError do
    @enforce_keys [:message]
    defexception [:message, :exception]
    @type t :: %__MODULE__{message: String.t(), exception: nil | Exception.t()}
  end

  defmodule EventStructs do
    defmacro __using__(structs_module: structs_mod) do
      quote do
        @behaviour Equinox.Codec
        @structs_module unquote(structs_mod)

        @impl Equinox.Codec
        def encode(%{__struct__: struct} = event, _context) do
          Equinox.Codec.EventStructs.struct_to_event_data(event, @structs_module)
        end

        @impl Equinox.Codec
        def decode(event) do
          Equinox.Codec.EventStructs.timeline_event_to_struct(event, @structs_module)
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
           message:
             "#{inspect(__MODULE__)}.encode: Expected a struct under #{parent_type}, got #{full_type}"
         }}
      end
    end

    def struct_to_event_data(not_struct, _) do
      {:error,
       %CodecError{
         message: "#{inspect(__MODULE__)}.encode: Expected struct, got #{inspect(not_struct)}"
       }}
    end

    def timeline_event_to_struct(%{type: type, data: data}, parent_module) do
      try do
        module = String.to_existing_atom("#{Atom.to_string(parent_module)}.#{type}")
        {:ok, struct!(module, for({k, v} <- data, do: {String.to_existing_atom(k), v}))}
      rescue
        exception in [ArgumentError] ->
          {:error,
           %CodecError{
             message: "#{inspect(__MODULE__)}.decode: #{inspect(exception)}"
           }}
      end
    end
  end
end
