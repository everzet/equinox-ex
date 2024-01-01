defmodule Equinox.Decider.Errors do
  defmodule ExhaustedLoadAttempts do
    defexception [:message, :stream_name, :attempts, :exception]

    @type t :: %__MODULE__{
            message: String.t(),
            stream_name: String.t(),
            attempts: pos_integer(),
            exception: Exception.t()
          }

    def exception(opts) do
      stream_name = Keyword.fetch!(opts, :stream_name)
      attempts = Keyword.fetch!(opts, :attempts)
      exception = Keyword.fetch!(opts, :exception)

      message =
        "Load from #{inspect(stream_name)} failed after #{attempts} attempt(s): #{Exception.message(exception)}"

      %__MODULE__{
        message: message,
        stream_name: stream_name,
        exception: exception,
        attempts: attempts
      }
    end
  end

  defmodule ExhaustedSyncAttempts do
    defexception [:message, :stream_name, :attempts, :exception]

    @type t :: %__MODULE__{
            message: String.t(),
            stream_name: String.t(),
            attempts: pos_integer(),
            exception: Exception.t()
          }

    def exception(opts) do
      stream_name = Keyword.fetch!(opts, :stream_name)
      attempts = Keyword.fetch!(opts, :attempts)
      exception = Keyword.fetch!(opts, :exception)

      message =
        "Sync to #{stream_name} failed after #{attempts} attempt(s): #{Exception.message(exception)}"

      %__MODULE__{
        message: message,
        stream_name: stream_name,
        exception: exception,
        attempts: attempts
      }
    end
  end

  defmodule ExhaustedResyncAttempts do
    defexception [:message, :stream_name, :attempts, :exception]

    @type t :: %__MODULE__{
            message: String.t(),
            stream_name: String.t(),
            attempts: non_neg_integer(),
            exception: Exception.t()
          }

    def exception(opts) do
      stream_name = Keyword.fetch!(opts, :stream_name)
      attempts = Keyword.fetch!(opts, :attempts)
      exception = Keyword.fetch!(opts, :exception)

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

  @type t :: ExhaustedLoadAttempts.t() | ExhaustedSyncAttempts.t() | ExhaustedResyncAttempts.t()
end
