defmodule Equinox.Store do
  alias Equinox.Decider.LoadPolicy
  alias Equinox.Store.{State, EventsToSync}

  @type t :: module()
  @type stream_name :: String.t()
  @type options :: keyword()

  @callback load(stream_name(), LoadPolicy.t(), options()) ::
              {:ok, State.t()}
              | {:error, Exception.t()}

  @callback sync(stream_name(), State.t(), EventsToSync.t(), options()) ::
              {:ok, State.t()}
              | {:error, Exception.t()}
              | {:conflict, resync :: (-> {:ok, State.t()} | {:error, Exception.t()})}
end
