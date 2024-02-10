defmodule Equinox.Decider.LoadPolicy do
  defstruct max_cache_age: 0, requires_leader?: false, assumes_empty?: false

  @type t :: %__MODULE__{
          max_cache_age: timeout(),
          requires_leader?: boolean(),
          assumes_empty?: boolean()
        }
  @type option ::
          :default
          | :assume_empty
          | :require_load
          | :require_leader
          | :any_cached_value
          | nonempty_list(
              {:assume_empty?, boolean()}
              | {:requires_leader?, boolean()}
              | {:max_cache_age, timeout()}
            )

  def new(%__MODULE__{} = policy), do: policy
  def new(:default), do: new(:require_load)
  def new(:assume_empty), do: %__MODULE__{assumes_empty?: true}
  def new(:require_load), do: %__MODULE__{max_cache_age: 0}
  def new(:require_leader), do: %__MODULE__{requires_leader?: true}
  def new(:any_cached_value), do: %__MODULE__{max_cache_age: :infinity}
  def new(opts) when is_list(opts), do: struct!(__MODULE__, opts)
end
