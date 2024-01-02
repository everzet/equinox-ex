defmodule Equinox.Codec.Errors do
  defmodule EncodeError do
    @enforce_keys [:message]
    defexception [:message, :exception]
    @type t :: %__MODULE__{message: String.t(), exception: nil | Exception.t()}
  end

  defmodule DecodeError do
    @enforce_keys [:message]
    defexception [:message, :exception]
    @type t :: %__MODULE__{message: String.t(), exception: nil | Exception.t()}
  end

  @type t :: EncodeError.t() | DecodeError.t()
end
