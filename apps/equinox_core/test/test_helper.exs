defmodule DeciderProcessMocks do
  def attach_mocks(mocks) do
    :telemetry.attach(
      {__MODULE__, mocks},
      [:equinox, :decider, :server, :init],
      &__MODULE__.handle_event/4,
      %{mocks: mocks}
    )
  end

  def handle_event(_even_name, _event_measurements, %{decider: decider}, %{mocks: mocks}) do
    with %{context: %{allow_mocks_from: manager_pid}} when is_pid(manager_pid) <- decider do
      for mock <- mocks do
        Mox.allow(mock, manager_pid, self())
      end
    end

    :ok
  end
end

Mox.defmock(Equinox.TestMocks.LifetimeMock, for: Equinox.Lifetime)
Mox.defmock(Equinox.TestMocks.StoreMock, for: Equinox.Store)
Mox.defmock(Equinox.TestMocks.CodecMock, for: Equinox.Codec)
Mox.defmock(Equinox.TestMocks.FoldMock, for: Equinox.Fold)

DeciderProcessMocks.attach_mocks([
  Equinox.TestMocks.LifetimeMock,
  Equinox.TestMocks.StoreMock,
  Equinox.TestMocks.CodecMock,
  Equinox.TestMocks.FoldMock
])

ExUnit.start()
