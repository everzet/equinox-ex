defmodule Equinox.Codec.EventStructs do
  defmacro __using__(_opts) do
    quote do
      @behaviour Equinox.Codec
      alias Equinox.Codec.EventStructs.{Upcast, Downcast}

      @impl Equinox.Codec
      def encode(%{__struct__: struct} = event, context) do
        event
        |> Equinox.Codec.EventStructs.struct_to_event_data(__MODULE__)
        |> Map.put(:metadata, context[:metadata])
      end

      @impl Equinox.Codec
      def decode(event) do
        Equinox.Codec.EventStructs.timeline_event_to_struct(event, __MODULE__)
      end

      defoverridable encode: 2
      defoverridable decode: 1
    end
  end

  defprotocol Upcast do
    @fallback_to_any true
    def upcast(struct)
  end

  defprotocol Downcast do
    @fallback_to_any true
    def downcast(struct)
  end

  defimpl Upcast, for: Any do
    def upcast(struct), do: struct
  end

  defimpl Downcast, for: Any do
    def downcast(struct), do: struct
  end

  def struct_to_event_data(%{__struct__: module} = event, parent_module) do
    parent_type = Atom.to_string(parent_module)
    full_type = Atom.to_string(module)

    if String.starts_with?(full_type, parent_type) do
      type = String.replace_leading(full_type, parent_type <> ".", "")

      data =
        event
        |> Downcast.downcast()
        |> Map.from_struct()
        |> Map.new(fn {k, v} -> {Atom.to_string(k), v} end)

      Equinox.Events.EventData.new(type: type, data: data)
    else
      raise ArgumentError, "Expected a struct under #{parent_type}, got #{full_type}"
    end
  end

  def timeline_event_to_struct(%{type: type, data: data}, parent_module) do
    "#{Atom.to_string(parent_module)}.#{type}"
    |> String.to_existing_atom()
    |> struct!(for({k, v} <- data, do: {String.to_existing_atom(k), v}))
    |> Upcast.upcast()
  end
end
