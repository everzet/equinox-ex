defprotocol Equinox.Cache do
  alias Equinox.Store

  @type t :: any()
  @type max_age :: non_neg_integer() | :infinity

  @spec fetch(t(), Store.stream_name(), max_age()) :: nil | Store.State.t()
  def fetch(cache, stream_name, max_age)

  @spec insert(t(), Store.stream_name(), Store.State.t()) :: :ok
  def insert(cache, stream_name, state)
end
