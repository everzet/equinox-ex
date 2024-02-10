defmodule Equinox.Store.State do
  alias Equinox.Fold

  defstruct [:value, :version]

  @type t :: %__MODULE__{value: Fold.result(), version: stream_version()}
  @type stream_version :: -1 | non_neg_integer()

  @spec new(Fold.result(), stream_version()) :: t()
  def new(value, version), do: %__MODULE__{value: value, version: version}
end
