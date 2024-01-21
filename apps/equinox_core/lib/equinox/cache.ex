defprotocol Equinox.Cache do
  alias Equinox.Store

  @type t :: any()
  @type max_age :: non_neg_integer() | :infinity

  @spec get(t(), Store.stream_name(), max_age()) :: nil | Store.State.t()
  def get(cache, stream_name, max_age)

  @spec put(t(), Store.stream_name(), Store.State.t()) :: :ok
  def put(cache, stream_name, state)
end
