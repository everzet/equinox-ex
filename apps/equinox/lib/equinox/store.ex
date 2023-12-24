defmodule Equinox.Store do
  alias Equinox.Stream.StreamName

  @type t :: module()
  @type stream_version :: -1 | non_neg_integer()
  @type written_position :: non_neg_integer()

  @callback load!(StreamName.t(), State.t(), Codec.t(), Fold.t()) :: State.t()
  @callback sync!(
              StreamName.t(),
              State.t(),
              list(DomainEvent.t()),
              Codec.ctx(),
              Codec.t(),
              Fold.t()
            ) :: State.t()

  defmodule DuplicateMessageId do
    defexception message: "Message with given ID already exists"
    @type t :: %__MODULE__{message: String.t()}
  end

  defmodule StreamVersionConflict do
    defexception message: "Wrong expected version"
    @type t :: %__MODULE__{message: String.t()}
  end
end
