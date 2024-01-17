defmodule Equinox.Store.LoadPolicy do
  defstruct max_age: 0, require_leader: false, assume_empty: false

  @type t :: %__MODULE__{
          max_age: non_neg_integer() | :infinity,
          require_leader: boolean(),
          assume_empty: boolean()
        }

  def default, do: require_load()
  def require_load, do: %__MODULE__{max_age: 0}
  def require_leader, do: %__MODULE__{require_leader: true}
  def any_cached_value, do: %__MODULE__{max_age: :inifinity}
  def allow_stale(max_age), do: %__MODULE__{max_age: max_age}
  def assume_empty, do: %__MODULE__{assume_empty: true}
end
