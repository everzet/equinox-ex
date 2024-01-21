defmodule Equinox.Codec.PassThroughData do
  @behaviour Equinox.Codec

  @impl Equinox.Codec
  def encode(data, context) do
    Equinox.Events.EventData.new(
      type: Map.get(data, "type", "unknown"),
      data: data,
      metadata: context[:metadata]
    )
  end

  @impl Equinox.Codec
  def decode(timeline_event) do
    timeline_event.data
  end
end
