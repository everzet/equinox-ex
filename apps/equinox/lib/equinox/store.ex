defmodule Equinox.Store do
  @type t :: module()
  @type stream :: String.t()

  @callback load!(stream(), State.t(), Codec.t(), Fold.t()) :: State.t()
  @callback sync!(stream(), State.t(), list(DomainEvent.t()), Codec.ctx(), Codec.t(), Fold.t()) ::
              State.t()

  defmodule DuplicateMessageId do
    defexception message: "Message with given ID already exists"
    @type t :: %__MODULE__{message: String.t()}
  end

  defmodule StreamVersionConflict do
    defexception message: "Wrong expected version"
    @type t :: %__MODULE__{message: String.t()}
  end
end
