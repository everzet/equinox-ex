defmodule Equinox.Decider.LifetimePolicy do
  defstruct after_init: 0, after_query: 0, after_transact: 0

  @type t :: %__MODULE__{after_init: timeout(), after_query: timeout(), after_transact: timeout()}
  @type option :: :default | {:max_inactivity, timeout()}

  def new(:default), do: new({:max_inactivity, :timer.minutes(20)})
  def new(fields) when is_list(fields), do: struct!(__MODULE__, fields)

  def new({:max_inactivity, timeout}) do
    %__MODULE__{after_init: timeout, after_query: timeout, after_transact: timeout}
  end
end
