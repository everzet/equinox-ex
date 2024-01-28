defprotocol Equinox.Cache do
  alias Equinox.Store
  alias Equinox.Codec.StreamName

  @type t :: any()

  @spec get(t(), StreamName.t(), timeout()) :: nil | Store.State.t()
  def get(cache, stream_name, max_age)

  @spec put(t(), StreamName.t(), Store.State.t()) :: :ok
  def put(cache, stream_name, state)
end
