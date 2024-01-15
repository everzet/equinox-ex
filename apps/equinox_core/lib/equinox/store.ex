defmodule Equinox.Store do
  alias Equinox.Store.{State, Outcome, StreamVersionConflict}

  @type t :: module()
  @type stream_name :: String.t()
  @type stream_version :: term()
  @type sync_context :: map()
  @type options :: keyword()

  @callback load(stream_name(), nil | State.t(), options()) ::
              {:ok, State.t()}
              | {:error, Exception.t(), partial :: State.t()}

  @callback sync(stream_name(), State.t(), Outcome.t(), options()) ::
              {:ok, State.t()}
              | {:error, StreamVersionConflict.t()}
              | {:error, Exception.t()}
end
