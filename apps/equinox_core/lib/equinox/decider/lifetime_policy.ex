defmodule Equinox.Decider.LifetimePolicy do
  @enforce_keys [:after_init, :after_query, :after_transact]
  defstruct [:after_init, :after_query, :after_transact]

  @type t :: %__MODULE__{
          after_init: timeout(),
          after_query: timeout(),
          after_transact: timeout()
        }

  def default, do: max_inactivity(:timer.seconds(30))

  def max_inactivity(timeout),
    do: %__MODULE__{after_init: timeout, after_query: timeout, after_transact: timeout}
end
