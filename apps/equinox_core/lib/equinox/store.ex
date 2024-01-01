defmodule Equinox.Store do
  @type t :: module()
  @type stream_name :: String.t()
  @type stream_version :: -1 | non_neg_integer()

  @callback load!(stream_name(), State.t(), Codec.t(), Fold.t()) :: State.t()
  @callback sync!(
              stream_name(),
              State.t(),
              list(DomainEvent.t()),
              Codec.context(),
              Codec.t(),
              Fold.t()
            ) :: State.t()
end
