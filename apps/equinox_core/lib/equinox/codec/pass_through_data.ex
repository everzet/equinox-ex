defmodule Equinox.Codec.PassThroughData do
  @behaviour Equinox.Codec

  @impl Equinox.Codec
  def encode(data, _ctx) do
    type = Map.get(data, "type", "unknown")
    {:ok, Equinox.Events.EventData.new(type: type, data: data)}
  end

  @impl Equinox.Codec
  def decode(timeline_event) do
    {:ok, timeline_event.data}
  end
end
