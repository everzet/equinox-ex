defmodule Equinox.Store do
  alias Equinox.{Codec, Fold}
  alias Equinox.Store.{State, Outcome, StreamVersionConflict}

  @type t :: module()
  @type stream_name :: String.t()
  @type stream_version :: term()

  @callback load(stream_name(), nil | State.t(), Codec.t(), Fold.t()) ::
              {:ok, State.t()}
              | {:error, Exception.t(), partial :: State.t()}

  @callback sync(stream_name(), State.t(), Outcome.t(), Codec.t(), Fold.t()) ::
              {:ok, State.t()}
              | {:error, StreamVersionConflict.t() | Exception.t()}
end
