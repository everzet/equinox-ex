defmodule Equinox.Decider.LoadPolicy do
  alias Equinox.Cache

  defstruct max_cache_age: 0, requires_leader?: false, assumes_empty?: false

  @type t :: %__MODULE__{
          max_cache_age: Cache.max_age(),
          requires_leader?: boolean(),
          assumes_empty?: boolean()
        }

  def default, do: require_load()
  def require_load, do: %__MODULE__{max_cache_age: 0}
  def require_leader, do: %__MODULE__{requires_leader?: true, max_cache_age: 0}
  def any_cached_value, do: %__MODULE__{max_cache_age: :inifinity}
  def allow_stale(max_cache_age), do: %__MODULE__{max_cache_age: max_cache_age}
  def assume_empty, do: %__MODULE__{assumes_empty?: true}
end
