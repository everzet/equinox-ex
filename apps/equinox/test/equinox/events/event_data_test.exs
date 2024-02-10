defmodule Equinox.Events.EventDataTest do
  use ExUnit.Case, async: true

  alias Equinox.Events.EventData
  alias Equinox.UUID

  @valid_opts [
    id: UUID.generate(),
    type: "Custom",
    data: %{val: :val},
    metadata: %{meta: :meta}
  ]

  test "is created using new/1 helper" do
    assert EventData.new(@valid_opts) == %EventData{
             id: @valid_opts[:id],
             type: "Custom",
             data: %{val: :val},
             metadata: %{meta: :meta}
           }
  end

  test "autogenerates ID if none is given" do
    assert EventData.new(type: "Custom").id
  end

  test "allows updating data via update_data/2 helper" do
    data = EventData.new(type: "Custom", data: %{val: :val})
    assert EventData.update_data(data, fn %{val: :val} -> :data end).data == :data
  end

  test "allows updating metadata via update_metadata/2 helper" do
    data = EventData.new(type: "Custom", metadata: %{meta: :meta})
    assert EventData.update_metadata(data, fn %{meta: :meta} -> :meta end).metadata == :meta
  end
end
