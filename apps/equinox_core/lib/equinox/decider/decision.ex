defmodule Equinox.Decider.Decision do
  alias Equinox.{Fold, Events.DomainEvent, Store.EventsToSync}

  @type fun_without_result ::
          (Fold.result() ->
             nil
             | DomainEvent.t()
             | list(DomainEvent.t())
             | {:ok, nil | DomainEvent.t() | list(DomainEvent.t())}
             | {:error, Error.t() | Exception.t() | term()})

  @type result :: term()
  @type fun_with_result ::
          (Fold.result() ->
             {result(), nil | DomainEvent.t() | list(DomainEvent.t())}
             | {:ok, result(), nil | DomainEvent.t() | list(DomainEvent.t())}
             | {:error, Error.t() | Exception.t() | term()})

  @type without_result :: fun_without_result() | {fun_without_result(), EventsToSync.context()}
  @type with_result :: fun_with_result() | {fun_with_result(), EventsToSync.context()}
  @type t :: without_result() | with_result()

  defmodule Error do
    @enforce_keys [:message]
    defexception [:message, :exception, :term]

    @type t :: %__MODULE__{
            message: String.t(),
            exception: nil | Exception.t(),
            term: nil | term()
          }

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
