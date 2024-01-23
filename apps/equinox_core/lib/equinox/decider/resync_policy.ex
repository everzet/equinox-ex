defmodule Equinox.Decider.ResyncPolicy do
  defmodule ExhaustedResyncAttempts do
    defexception [:message]
    @type t :: %__MODULE__{message: String.t()}
  end

  @enforce_keys [:max_attempts]
  defstruct [:max_attempts]

  @type t ::
          {:max_attempts, non_neg_integer()}
          | %__MODULE__{max_attempts: non_neg_integer()}

  def normalize(%__MODULE__{} = policy), do: policy
  def normalize({:max_attempts, attempts}), do: max_attempts(attempts)

  def default, do: max_attempts(3)
  def max_attempts(max_attempts), do: %__MODULE__{max_attempts: max_attempts}

  def validate_attempt(%__MODULE__{max_attempts: max_attempts}, attempt) do
    if attempt < max_attempts do
      :ok
    else
      {:error,
       ExhaustedResyncAttempts.exception(
         "Decider <-> Stream state version conflict. Aborting after #{attempt} attempt(s)"
       )}
    end
  end
end
