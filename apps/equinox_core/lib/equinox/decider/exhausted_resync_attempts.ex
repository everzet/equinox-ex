defmodule Equinox.Decider.ExhaustedResyncAttempts do
  defexception [:message, :stream_name, :attempts, :exception]

  @type t :: %__MODULE__{
          message: String.t(),
          stream_name: String.t(),
          attempts: non_neg_integer(),
          exception: Exception.t()
        }

  def exception(opts) do
    stream_name = Keyword.fetch!(opts, :stream_name)
    exception = Keyword.fetch!(opts, :exception)
    attempts = Keyword.fetch!(opts, :attempts)

    message =
      "Failed to resync with #{stream_name} after #{attempts} attempt(s): #{Exception.message(exception)}"

    %__MODULE__{
      message: message,
      stream_name: stream_name,
      exception: exception,
      attempts: attempts
    }
  end
end
