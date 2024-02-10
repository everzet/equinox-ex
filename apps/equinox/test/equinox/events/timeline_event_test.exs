defmodule Equinox.Events.TimelineEventTest do
  use ExUnit.Case, async: true

  alias Equinox.Events.TimelineEvent
  alias Equinox.UUID

  @valid_opts [
    id: UUID.generate(),
    type: "Custom",
    stream_name: "Stream-1",
    position: 2,
    global_position: 3,
    data: %{val: :val},
    metadata: %{meta: :meta},
    time: NaiveDateTime.utc_now()
  ]

  test "is created using new/1 helper" do
    assert TimelineEvent.new(@valid_opts) == %TimelineEvent{
             id: @valid_opts[:id],
             time: @valid_opts[:time],
             type: "Custom",
             stream_name: "Stream-1",
             position: 2,
             global_position: 3,
             data: %{val: :val},
             metadata: %{meta: :meta}
           }
  end

  test "allows updating data via update_data/2 helper" do
    data = TimelineEvent.new(@valid_opts)
    assert TimelineEvent.update_data(data, fn %{val: :val} -> :data end).data == :data
  end

  test "allows updating metadata via update_metadata/2 helper" do
    data = TimelineEvent.new(@valid_opts)
    assert TimelineEvent.update_metadata(data, fn %{meta: :meta} -> :meta end).metadata == :meta
  end
end
