defmodule Equinox.Store.StreamVersionConflict do
  alias Equinox.Store

  defexception [:message, :stream_name, :stream_version]

  @type t :: %__MODULE__{
          message: String.t(),
          stream_name: nil | Store.stream_name(),
          stream_version: nil | Store.stream_version()
        }
end
