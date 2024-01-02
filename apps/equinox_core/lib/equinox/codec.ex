defmodule Equinox.Codec do
  alias Equinox.Telemetry
  alias Equinox.Events.{DomainEvent, EventData, TimelineEvent}
  alias Equinox.Codec.Errors

  @type t :: module()
  @type context :: map()

  @callback encode(DomainEvent.t(), context()) :: {:ok, EventData.t()} | {:error, Errors.t()}
  @callback decode(TimelineEvent.t()) :: {:ok, DomainEvent.t()} | {:error, Errors.t()}

  @spec encode!(DomainEvent.t(), context(), t()) :: EventData.t()
  def encode!(domain_event, context, codec) do
    Telemetry.span_codec_encode(codec, domain_event, context, fn ->
      case codec.encode(domain_event, context) do
        {:ok, timeline_event} -> timeline_event
        {:error, exception} when is_exception(exception) -> raise exception
        {:error, term} -> raise Errors.EncodeError, message: inspect(term)
      end
    end)
  rescue
    wrapped in [Errors.EncodeError] ->
      reraise wrapped, __STACKTRACE__

    unwrapped ->
      reraise Errors.EncodeError,
              [
                message: "#{inspect(codec)}: #{Exception.message(unwrapped)}",
                exception: unwrapped
              ],
              __STACKTRACE__
  end

  @spec decode!(TimelineEvent.t(), t()) :: DomainEvent.with_position()
  def decode!(timeline_event, codec) do
    Telemetry.span_codec_decode(codec, timeline_event, fn ->
      case codec.decode(timeline_event) do
        {:ok, domain_event} -> {domain_event, timeline_event.position}
        {:error, exception} when is_exception(exception) -> raise exception
        {:error, term} -> raise Errors.DecodeError, message: inspect(term)
      end
    end)
  rescue
    wrapped in [Errors.DecodeError] ->
      reraise wrapped, __STACKTRACE__

    unwrapped ->
      reraise Errors.DecodeError,
              [
                message: "#{inspect(codec)}: #{Exception.message(unwrapped)}",
                exception: unwrapped
              ],
              __STACKTRACE__
  end

  @spec encode_list!(list(DomainEvent.t()), context(), t()) :: list(EventData.t())
  def encode_list!(events, context, codec) when is_list(events) do
    Enum.map(events, &encode!(&1, context, codec))
  end

  @spec decode_stream!(Enumerable.t(TimelineEvent.t()), t()) ::
          Enumerable.t(DomainEvent.with_position())
  def decode_stream!(events, codec) do
    Stream.map(events, &decode!(&1, codec))
  end
end
