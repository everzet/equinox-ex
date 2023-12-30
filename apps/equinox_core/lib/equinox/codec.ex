defmodule Equinox.Codec do
  alias Equinox.Telemetry
  alias Equinox.Events.{DomainEvent, EventData, TimelineEvent}

  @type t :: module()
  @type ctx :: any()

  @callback encode(DomainEvent.t(), ctx()) :: {:ok, EventData.t()} | {:error, CodecError.t()}
  @callback decode(TimelineEvent.t()) :: {:ok, DomainEvent.t()} | {:error, CodecError.t()}

  defmodule CodecError do
    @enforce_keys [:message]
    defexception [:message, :exception]
    @type t :: %__MODULE__{message: String.t(), exception: nil | Exception.t()}
  end

  @spec encode!(list(DomainEvent.t()), ctx(), t()) :: list(EventData.t())
  def encode!(events, ctx, codec) when is_list(events) do
    Enum.map(events, &encode!(&1, ctx, codec))
  end

  @spec encode!(DomainEvent.t(), ctx(), t()) :: EventData.t()
  def encode!(domain_event, ctx, codec) do
    try do
      Telemetry.span_codec_encode(codec, domain_event, ctx, fn ->
        case codec.encode(domain_event, ctx) do
          {:ok, timeline_event} -> timeline_event
          {:error, exception} when is_exception(exception) -> raise exception
          {:error, term} -> raise RuntimeError, message: inspect(term)
        end
      end)
    rescue
      exception in [CodecError] ->
        reraise exception, __STACKTRACE__

      exception ->
        reraise CodecError,
                [
                  message: "#{inspect(codec)}.encode: #{Exception.message(exception)}",
                  exception: exception
                ],
                __STACKTRACE__
    end
  end

  @spec decode_with_position!(Enumerable.t(TimelineEvent.t()), t()) ::
          Enumerable.t(DomainEvent.with_position())
  def decode_with_position!(events, codec)
      # enumerable can be any of these
      when is_struct(events, Stream) or
             is_function(events) or
             is_list(events) do
    Stream.map(events, &decode_with_position!(&1, codec))
  end

  @spec decode_with_position!(TimelineEvent.t(), t()) :: DomainEvent.with_position()
  def decode_with_position!(timeline_event, codec) do
    try do
      Telemetry.span_codec_decode(codec, timeline_event, fn ->
        case codec.decode(timeline_event) do
          {:ok, domain_event} -> {domain_event, timeline_event.position}
          {:error, exception} when is_exception(exception) -> raise exception
          {:error, term} -> raise RuntimeError, message: inspect(term)
        end
      end)
    rescue
      exception in [CodecError] ->
        reraise exception, __STACKTRACE__

      exception ->
        reraise CodecError,
                [
                  message: "#{inspect(codec)}.decode: #{Exception.message(exception)}",
                  exception: exception
                ],
                __STACKTRACE__
    end
  end

  defmodule PassThroughData do
    @behaviour Equinox.Codec

    @impl Equinox.Codec
    def encode(data, _ctx) do
      type = Map.get(data, "type", "unknown")
      {:ok, EventData.new(type: type, data: data)}
    end

    @impl Equinox.Codec
    def decode(timeline_event) do
      {:ok, timeline_event.data}
    end
  end

  defmodule EventStructs do
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

        struct =
          module
          |> struct!(for({k, v} <- data, do: {String.to_existing_atom(k), v}))
          |> Upcast.upcast()

        {:ok, struct}
      rescue
        exception in [ArgumentError] ->
          {:error,
           %CodecError{
             message: "#{inspect(__MODULE__)}.decode: #{Exception.message(exception)}"
           }}
      end
    end
  end
end
