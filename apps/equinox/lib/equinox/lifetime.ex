defmodule Equinox.Lifetime do
  alias Equinox.Fold

  @type t :: module()
  @callback after_init(Fold.state()) :: timeout()
  @callback after_query(Fold.state()) :: timeout()
  @callback after_transact(Fold.state()) :: timeout()

  defmodule Default do
    @behaviour Equinox.Lifetime

    @timeout :timer.seconds(30)

    def after_init(_), do: @timeout
    def after_query(_), do: @timeout
    def after_transact(_), do: @timeout
  end
end
