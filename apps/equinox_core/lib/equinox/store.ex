defprotocol Equinox.Store do
  alias Equinox.Decider.LoadPolicy
  alias Equinox.Store.{State, EventsToSync}

  @type t :: any()
  @type stream_name :: String.t()

  @spec load(t(), String.t(), LoadPolicy.t()) ::
          {:ok, State.t()}
          | {:error, Exception.t()}
  def load(store, stream_name, load_policy)

  @spec sync(t(), String.t(), State.t(), EventsToSync.t()) ::
          {:ok, State.t()}
          | {:error, Exception.t()}
          | {:conflict, resync :: (-> {:ok, State.t()} | {:error, Exception.t()})}
  def sync(store, stream_name, origin_state, events_to_sync)
end
