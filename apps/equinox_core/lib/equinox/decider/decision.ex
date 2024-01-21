defmodule Equinox.Decider.Decision do
  alias Equinox.{Fold, Store.EventsToSync}

  defmodule Error do
    @enforce_keys [:message]
    defexception [:message, :exception, :term]

    @type unwrapped :: Error.t() | Exception.t() | term() | String.t()
    @type t :: %__MODULE__{
            message: String.t(),
            exception: nil | Exception.t(),
            term: nil | term()
          }

    @spec exception(unwrapped()) :: t()
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
             | {:error, Error.unwrapped()})

  @type fun_with_result ::
          (Fold.result() ->
             {result(), EventsToSync.events()}
             | {:ok, result(), EventsToSync.events()}
             | {:error, Error.unwrapped()})

  @spec execute(without_result(), Fold.result()) ::
          {:ok, :ok, EventsToSync.t()}
          | {:error, Error.t()}
  @spec execute(with_result(), Fold.result()) ::
          {:ok, {:ok, term()}, EventsToSync.t()}
          | {:error, Error.t()}
  def execute(decision, state) do
    {decision, context} =
      case decision do
        {decision, context} -> {decision, context}
        decision -> {decision, %{}}
      end

    case decision.(state) do
      {:error, error} -> {:error, Error.exception(error)}
      {:ok, events} -> {:ok, :ok, EventsToSync.new(events, context)}
      {:ok, result, events} -> {:ok, {:ok, result}, EventsToSync.new(events, context)}
      {result, events} -> {:ok, {:ok, result}, EventsToSync.new(events, context)}
      events -> {:ok, :ok, EventsToSync.new(events, context)}
    end
  end
end
