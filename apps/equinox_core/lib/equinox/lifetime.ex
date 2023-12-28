defmodule Equinox.Lifetime do
  alias Equinox.State

  @type t :: module()
  @callback after_init(State.value()) :: timeout()
  @callback after_query(State.value()) :: timeout()
  @callback after_transact(State.value()) :: timeout()

  defmodule StayAliveFor30Seconds do
    @behaviour Equinox.Lifetime
    def after_init(_), do: :timer.seconds(30)
    def after_query(_), do: :timer.seconds(30)
    def after_transact(_), do: :timer.seconds(30)
  end
end
