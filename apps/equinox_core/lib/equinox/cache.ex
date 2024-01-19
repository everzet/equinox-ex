defmodule Equinox.Cache do
  alias Equinox.Store

  @type t :: module()
  @type max_age :: :infinity | non_neg_integer()

  @callback fetch(Store.stream_name(), max_age()) :: nil | State.t()
  @callback insert(Store.stream_name(), State.t()) :: :ok
end
