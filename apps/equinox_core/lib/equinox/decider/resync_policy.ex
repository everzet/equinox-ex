defmodule Equinox.Decider.ResyncPolicy do
  defstruct max_attempts: 3
  @type t :: %__MODULE__{max_attempts: non_neg_integer()}

  defmodule ExhaustedResyncAttempts do
    alias Equinox.Store.StreamVersionConflict

    defexception [:message, :stream_name, :attempts, :conflict]

    @type t :: %__MODULE__{
            message: String.t(),
            stream_name: nil | String.t(),
            attempts: non_neg_integer(),
            conflict: StreamVersionConflict.t()
          }

    def exception(opts) do
      conflict = Keyword.fetch!(opts, :conflict)
      attempts = Keyword.fetch!(opts, :attempts)
      stream_name = conflict.stream_name

      %__MODULE__{
        message:
          "Failed to resync with stream '#{stream_name}' after #{attempts} attempt(s): #{Exception.message(conflict)}",
        stream_name: stream_name,
        conflict: conflict,
        attempts: attempts
      }
    end
  end

  def max_attempts(max_attempts), do: %__MODULE__{max_attempts: max_attempts}

  def validate!(%__MODULE__{max_attempts: max_attempts}, attempts, conflict) do
    if attempts >= max_attempts do
      raise ExhaustedResyncAttempts, conflict: conflict, attempts: attempts
    end
  end
end
