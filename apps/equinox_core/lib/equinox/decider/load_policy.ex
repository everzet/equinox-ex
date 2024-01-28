defmodule Equinox.Decider.LoadPolicy do
  defstruct max_cache_age: 0, requires_leader?: false, assumes_empty?: false

  @type t :: %__MODULE__{
          max_cache_age: timeout(),
          requires_leader?: boolean(),
          assumes_empty?: boolean()
        }
  @type option ::
          :default
          | :require_load
          | :require_leader
          | :any_cached_value
          | {:allow_stale, pos_integer()}
          | :assume_empty

  def new(%__MODULE__{} = policy), do: policy
  def new(:default), do: new(:require_load)
  def new(:require_load), do: %__MODULE__{max_cache_age: 0}
  def new(:require_leader), do: %__MODULE__{requires_leader?: true, max_cache_age: 0}
  def new(:any_cached_value), do: %__MODULE__{max_cache_age: :inifinity}
  def new({:allow_stale, max_cache_age}), do: %__MODULE__{max_cache_age: max_cache_age}
  def new(:assume_empty), do: %__MODULE__{assumes_empty?: true}
end
