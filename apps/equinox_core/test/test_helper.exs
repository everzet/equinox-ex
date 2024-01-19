defmodule DeciderProcessMocks do
  def attach_mocks(mocks) do
    :telemetry.attach(
      {__MODULE__, mocks},
      [:equinox, :decider, :async, :init],
      &__MODULE__.handle_event/4,
      %{mocks: mocks}
    )
  end

  def handle_event(_even_name, _event_measurements, %{decider: decider}, %{mocks: mocks}) do
    with %{store: %Equinox.StoreMock.Config{allow_from: test_pid}} <- decider do
      for mock <- mocks do
        Mox.allow(mock, test_pid, self())
      end
    end

    :ok
  end
end

DeciderProcessMocks.attach_mocks([Equinox.StoreMock])

ExUnit.start()
