defmodule Equinox.Decider.ResyncPolicy do
  defmodule ExhaustedResyncAttempts do
    defexception [:message]
    @type t :: %__MODULE__{message: String.t()}
  end

  @enforce_keys [:max_attempts]
  defstruct [:max_attempts]

  @type t :: %__MODULE__{max_attempts: non_neg_integer()}
  @type option :: :default | {:max_attempts, non_neg_integer()}

  def new(%__MODULE__{} = policy), do: policy
  def new(:default), do: new({:max_attempts, 3})
  def new({:max_attempts, attempts}), do: %__MODULE__{max_attempts: attempts}

  def validate_resync_attempt(%__MODULE__{max_attempts: max_attempts}, attempt) do
    if attempt <= max_attempts do
      :ok
    else
      {:error,
       ExhaustedResyncAttempts.exception(
         "Decider <-> Stream state version conflict. Aborting after #{attempt - 1} attempt(s) to resync"
       )}
    end
  end
end
