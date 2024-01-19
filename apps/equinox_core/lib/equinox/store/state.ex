defmodule Equinox.Store.State do
  alias Equinox.Fold

  defstruct [:value, :version]
  @type t :: %__MODULE__{value: Fold.result(), version: stream_version()}
  @type stream_version :: term()

  @spec ensure_initialized(nil | t(), Fold.t(), stream_version()) :: t()
  def ensure_initialized(nil, fold, initial_version), do: init(fold, initial_version)
  def ensure_initialized(already_initialized, _, _), do: already_initialized

  @spec init(Fold.t(), stream_version()) :: t()
  def init(fold, version), do: fold.initial() |> new(version)

  @spec new(Fold.result(), stream_version()) :: t()
  def new(value, version), do: %__MODULE__{value: value, version: version}
end
