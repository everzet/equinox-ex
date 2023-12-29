defmodule Equinox.Store do
  @type t :: module()
  @type stream_version :: -1 | non_neg_integer()

  @callback load!(stream_name :: String.t(), State.t(), Codec.t(), Fold.t()) :: State.t()
  @callback sync!(
              stream_name :: String.t(),
              State.t(),
              list(DomainEvent.t()),
              Codec.ctx(),
              Codec.t(),
              Fold.t()
            ) :: State.t()

  defmodule DuplicateMessageId do
    defexception message: "Message with given ID already exists",
                 message_id: nil

    @type t :: %__MODULE__{message: String.t(), message_id: nil | String.t()}
  end

  defmodule StreamVersionConflict do
    defexception message: "Wrong expected version",
                 stream_name: nil,
                 stream_version: nil

    @type t :: %__MODULE__{
            message: String.t(),
            stream_name: nil | String.t(),
            stream_version: nil | Store.stream_version()
          }
  end
end
