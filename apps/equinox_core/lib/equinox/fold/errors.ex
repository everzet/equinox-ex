defmodule Equinox.Fold.Errors do
  defmodule FoldError do
    @enforce_keys [:message]
    defexception [:message, :exception]
    @type t :: %__MODULE__{message: String.t(), exception: nil | Exception.t()}
  end

  @type t :: FoldError.t()
end
