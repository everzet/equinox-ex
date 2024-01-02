defmodule Equinox.Store.Errors do
  defmodule DuplicateMessageId do
    defexception message: "Message with given ID already exists",
                 message_id: nil

    @type t :: %__MODULE__{message: String.t(), message_id: nil | String.t()}
  end

  defmodule StreamVersionConflict do
    alias Equinox.Store

    defexception message: "Wrong expected version",
                 stream_name: nil,
                 stream_version: nil

    @type t :: %__MODULE__{
            message: String.t(),
            stream_name: nil | Store.stream_name(),
            stream_version: nil | Store.stream_version()
          }
  end

  @type t :: DuplicateMessageId.t() | StreamVersionConflict.t()
end
