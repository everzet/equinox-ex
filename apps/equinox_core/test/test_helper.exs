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
    with %{store: {_, store_options}} <- decider,
         test_pid when is_pid(test_pid) <- store_options[:allow_mocks_from] do
      for mock <- mocks do
        Mox.allow(mock, test_pid, self())
      end
    end

    :ok
  end
end

Mox.defmock(Equinox.TestMocks.StoreMock, for: Equinox.Store)
Mox.defmock(Equinox.TestMocks.LifetimeMock, for: Equinox.Lifetime)

DeciderProcessMocks.attach_mocks([
  Equinox.TestMocks.StoreMock,
  Equinox.TestMocks.LifetimeMock
])

ExUnit.start()
