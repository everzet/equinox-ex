defmodule Equinox.Decider.Decision do
  alias Equinox.{Fold, Store.EventsToSync}

  defmodule Error do
    @enforce_keys [:message]
    defexception [:message, :exception, :term]

    @type t :: %__MODULE__{
            message: String.t(),
            exception: nil | Exception.t(),
            term: nil | term()
          }
    @type raw :: Exception.t() | term() | String.t()

    @spec exception(t() | raw()) :: t()
    def exception(error) do
      case error do
        %__MODULE__{} = already_error ->
          already_error

        message when is_bitstring(message) ->
          %__MODULE__{message: message}

        exception when is_exception(exception) ->
          %__MODULE__{message: Exception.message(exception), exception: exception}

        error_term ->
          %__MODULE__{message: inspect(error_term), term: error_term}
      end
    end
  end

  @type t :: without_result() | with_result()
  @type without_result :: fun_without_result() | {fun_without_result(), EventsToSync.context()}
  @type with_result :: fun_with_result() | {fun_with_result(), EventsToSync.context()}
  @type result :: term()

  @type fun_without_result ::
          (Fold.result() ->
             EventsToSync.events()
             | {:ok, EventsToSync.events()}
             | {:error, Error.t() | Error.raw()})

  @type fun_with_result ::
          (Fold.result() ->
             {result(), EventsToSync.events()}
             | {:ok, result(), EventsToSync.events()}
             | {:error, Error.t() | Error.raw()})

  @spec execute(without_result(), Fold.result()) ::
          {:ok, :ok, EventsToSync.t()}
          | {:error, Error.t()}
  @spec execute(with_result(), Fold.result()) ::
          {:ok, {:ok, result()}, EventsToSync.t()}
          | {:error, Error.t()}
  def execute({decision, context}, state), do: do_execute(decision, state, context)
  def execute(decision, state), do: do_execute(decision, state, %{})

  defp do_execute(decision, state, context) do
    case decision.(state) do
      {:error, error} -> {:error, Error.exception(error)}
      {:ok, events} -> {:ok, :ok, EventsToSync.new(events, context)}
      {:ok, result, events} -> {:ok, {:ok, result}, EventsToSync.new(events, context)}
      {result, events} -> {:ok, {:ok, result}, EventsToSync.new(events, context)}
      events -> {:ok, :ok, EventsToSync.new(events, context)}
    end
  end
end
