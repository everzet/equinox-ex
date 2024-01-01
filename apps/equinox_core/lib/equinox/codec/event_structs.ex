defmodule Equinox.Codec.EventStructs do
  alias Equinox.Codec.Errors

  defmacro __using__(_opts) do
    quote do
      @behaviour Equinox.Codec
      alias Equinox.Codec.EventStructs.Upcast

      @impl Equinox.Codec
      def encode(%{__struct__: struct} = event, _ctx) do
        Equinox.Codec.EventStructs.struct_to_event_data(event, __MODULE__)
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

  defimpl Upcast, for: Any do
    def upcast(struct), do: struct
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
       %Errors.CodecError{
         message:
           "#{inspect(__MODULE__)}.encode: Expected a struct under #{parent_type}, got #{full_type}"
       }}
    end
  end

  def struct_to_event_data(not_struct, _) do
    {:error,
     %Errors.CodecError{
       message: "#{inspect(__MODULE__)}.encode: Expected struct, got #{inspect(not_struct)}"
     }}
  end

  def timeline_event_to_struct(%{type: type, data: data}, parent_module) do
    try do
      module = String.to_existing_atom("#{Atom.to_string(parent_module)}.#{type}")

      struct =
        module
        |> struct!(for({k, v} <- data, do: {String.to_existing_atom(k), v}))
        |> Upcast.upcast()

      {:ok, struct}
    rescue
      exception in [ArgumentError] ->
        {:error,
         %Errors.CodecError{
           message: "#{inspect(__MODULE__)}.decode: #{Exception.message(exception)}"
         }}
    end
  end
end
