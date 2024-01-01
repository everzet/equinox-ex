defmodule Equinox.Store do
  alias Equinox.{State, Codec, Fold, Events}

  @type t :: module()
  @type stream_name :: String.t()
  @type stream_version :: -1 | non_neg_integer()

  @callback load!(stream_name(), State.t(), Codec.t(), Fold.t()) :: State.t()
  @callback sync!(
              stream_name(),
              State.t(),
              list(Events.DomainEvent.t()),
              Codec.context(),
              Codec.t(),
              Fold.t()
            ) :: State.t()
end
