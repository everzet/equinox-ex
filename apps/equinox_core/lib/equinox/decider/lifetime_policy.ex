defmodule Equinox.Decider.LifetimePolicy do
  @enforce_keys [:after_init, :after_query, :after_transact]
  defstruct [:after_init, :after_query, :after_transact]

  @type t ::
          {:max_inactivity, timeout()}
          | %__MODULE__{after_init: timeout(), after_query: timeout(), after_transact: timeout()}

  def wrap(%__MODULE__{} = policy), do: policy
  def wrap({:max_inactivity, timeout}), do: max_inactivity(timeout)

  def default, do: max_inactivity(:timer.minutes(20))

  def max_inactivity(timeout),
    do: %__MODULE__{after_init: timeout, after_query: timeout, after_transact: timeout}
end
