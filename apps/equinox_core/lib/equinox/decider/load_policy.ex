defmodule Equinox.Decider.LoadPolicy do
  alias Equinox.Cache

  defstruct max_cache_age: 0, requires_leader?: false, assumes_empty?: false

  @type t ::
          :default
          | :require_load
          | :require_leader
          | :any_cached_value
          | {:allow_stale, non_neg_integer()}
          | :assume_empty
          | %__MODULE__{
              max_cache_age: Cache.max_age(),
              requires_leader?: boolean(),
              assumes_empty?: boolean()
            }

  def normalize(%__MODULE__{} = policy), do: policy
  def normalize({:allow_stale, max_cache_age}), do: allow_stale(max_cache_age)
  def normalize(fun) when is_atom(fun), do: apply(__MODULE__, fun, [])

  def default, do: require_load()
  def require_load, do: %__MODULE__{max_cache_age: 0}
  def require_leader, do: %__MODULE__{requires_leader?: true, max_cache_age: 0}
  def any_cached_value, do: %__MODULE__{max_cache_age: :inifinity}
  def allow_stale(max_cache_age), do: %__MODULE__{max_cache_age: max_cache_age}
  def assume_empty, do: %__MODULE__{assumes_empty?: true}
end
