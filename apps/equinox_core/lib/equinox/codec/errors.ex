defmodule Equinox.Codec.Errors do
  defmodule CodecError do
    @enforce_keys [:message]
    defexception [:message, :exception]
    @type t :: %__MODULE__{message: String.t(), exception: nil | Exception.t()}
  end

  @type t :: CodecError.t()
end
