defmodule Equinox.Decider.LifetimePolicy do
  defstruct after_init: 0, after_query: 0, after_transact: 0

  @type t :: %__MODULE__{after_init: timeout(), after_query: timeout(), after_transact: timeout()}
  @type option :: :default | keyword(timeout())

  def new(%__MODULE__{} = policy), do: policy
  def new(:default), do: new(max_inactivity: :timer.minutes(20))
  def new(max_inactivity: t), do: new(after_init: t, after_query: t, after_transact: t)
  def new(opts) when is_list(opts), do: struct!(__MODULE__, opts)
end
