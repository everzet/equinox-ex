defmodule Equinox.Decider.ResyncPolicy do
  defmodule MaxResyncsExhaustedError do
    defexception [:message]
    @type t :: %__MODULE__{message: String.t()}
  end

  @enforce_keys [:max_attempts]
  defstruct [:max_attempts]

  @type t :: %__MODULE__{max_attempts: non_neg_integer()}
  @type option :: :default | nonempty_list({:max_attempts, non_neg_integer()})

  def new(%__MODULE__{} = policy), do: policy
  def new(:default), do: new(max_attempts: 3)
  def new(opts) when is_list(opts), do: struct!(__MODULE__, opts)

  def validate_resync_attempt(%__MODULE__{max_attempts: max_attempts}, attempt) do
    if attempt <= max_attempts do
      :ok
    else
      {:error,
       MaxResyncsExhaustedError.exception(
         "Decider <-> Stream state version conflict. Aborting after #{attempt - 1} attempt(s) to resync"
       )}
    end
  end
end
