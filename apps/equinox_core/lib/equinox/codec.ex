defmodule Equinox.Codec do
  alias Equinox.Telemetry
  alias Equinox.Events.{DomainEvent, EventData, TimelineEvent}
  alias Equinox.Codec.Errors

  @type t :: module()
  @type context :: map()

  @callback encode(DomainEvent.t(), context()) :: {:ok, EventData.t()} | {:error, Errors.t()}
  @callback decode(TimelineEvent.t()) :: {:ok, DomainEvent.t()} | {:error, Errors.t()}

  @spec encode!(list(DomainEvent.t()), context(), t()) :: list(EventData.t())
  def encode!(events, context, codec) when is_list(events) do
    Enum.map(events, &encode!(&1, context, codec))
  end

  @spec encode!(DomainEvent.t(), context(), t()) :: EventData.t()
  def encode!(domain_event, context, codec) do
    try do
      Telemetry.span_codec_encode(codec, domain_event, context, fn ->
        case codec.encode(domain_event, context) do
          {:ok, timeline_event} -> timeline_event
          {:error, exception} when is_exception(exception) -> raise exception
          {:error, term} -> raise RuntimeError, message: inspect(term)
        end
      end)
    rescue
      exception in [Errors.CodecError] ->
        reraise exception, __STACKTRACE__

      exception ->
        reraise Errors.CodecError,
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
      exception in [Errors.CodecError] ->
        reraise exception, __STACKTRACE__

      exception ->
        reraise Errors.CodecError,
                [
                  message: "#{inspect(codec)}.decode: #{Exception.message(exception)}",
                  exception: exception
                ],
                __STACKTRACE__
    end
  end
end
