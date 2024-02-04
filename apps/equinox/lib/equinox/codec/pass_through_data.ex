defmodule Equinox.Codec.PassThroughData do
  @behaviour Equinox.Codec
  alias Equinox.Events.{EventData, TimelineEvent}

  @impl Equinox.Codec
  def encode(data, context) do
    EventData.new(type: Map.fetch!(data, "type"), data: data, metadata: context[:metadata])
  rescue
    KeyError ->
      reraise ArgumentError,
              "PassThroughData codec requires events to contain `type` field, but #{inspect(data)} has none",
              __STACKTRACE__
  end

  @impl Equinox.Codec
  def decode(%TimelineEvent{} = event), do: event.data
end
