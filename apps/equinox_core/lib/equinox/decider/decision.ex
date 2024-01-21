defmodule Equinox.Decider.Decision do
  alias Equinox.{Fold, Events.DomainEvent, Store.EventsToSync}

  @type fun_without_result ::
          (Fold.result() ->
             nil
             | DomainEvent.t()
             | list(DomainEvent.t())
             | {:ok, nil | DomainEvent.t() | list(DomainEvent.t())}
             | {:error, term()})

  @type result :: term()
  @type fun_with_result ::
          (Fold.result() ->
             {result(), nil | DomainEvent.t() | list(DomainEvent.t())}
             | {:ok, result(), nil | DomainEvent.t() | list(DomainEvent.t())}
             | {:error, term()})

  @type without_result :: fun_without_result() | {fun_without_result(), EventsToSync.context()}
  @type with_result :: fun_with_result() | {fun_with_result(), EventsToSync.context()}
  @type t :: without_result() | with_result()

  defmodule Error do
    defexception [:message, :term]
    @type t :: %__MODULE__{message: String.t(), term: term()}

    def exception(error_term) do
      %__MODULE__{message: "Decision error: #{inspect(error_term)}", term: error_term}
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
      {:ok, events} -> {:ok, :ok, wrap(events, context)}
      {:ok, result, events} -> {:ok, {:ok, result}, wrap(events, context)}
      {result, events} -> {:ok, {:ok, result}, wrap(events, context)}
      events -> {:ok, :ok, wrap(events, context)}
    end
  end

  defp wrap(nil_or_event_or_events, context) do
    nil_or_event_or_events
    |> List.wrap()
    |> EventsToSync.new(context)
  end
end
