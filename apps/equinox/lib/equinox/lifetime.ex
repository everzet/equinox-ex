defmodule Equinox.Lifetime do
  alias Equinox.Fold

  @type t :: module()
  @callback after_init(Fold.state()) :: timeout()
  @callback after_query(Fold.state()) :: timeout()
  @callback after_transact(Fold.state()) :: timeout()

  defmodule StayAliveFor30Seconds do
    @behaviour Equinox.Lifetime
    def after_init(_), do: :timer.seconds(30)
    def after_query(_), do: :timer.seconds(30)
    def after_transact(_), do: :timer.seconds(30)
  end
end
