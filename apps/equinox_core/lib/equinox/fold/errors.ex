defmodule Equinox.Fold.Errors do
  defmodule EvolveError do
    @enforce_keys [:message]
    defexception [:message, :exception]
    @type t :: %__MODULE__{message: String.t(), exception: nil | Exception.t()}
  end

  @type t :: EvolveError.t()
end
