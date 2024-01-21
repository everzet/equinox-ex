defmodule Equinox.Decider.Decision do
  alias Equinox.Fold
  alias Equinox.Events.DomainEvent
  alias Equinox.Store.EventsToSync

  @type without_result_fun ::
          (Fold.result() ->
             nil
             | DomainEvent.t()
             | list(DomainEvent.t())
             | {:ok, DomainEvent.t() | list(DomainEvent.t())}
             | {:error, term()})
  @type without_result :: without_result_fun() | {without_result_fun() | EventsToSync.context()}

  @type with_result_fun ::
          (Fold.result() ->
             {result :: term(), DomainEvent.t() | list(DomainEvent.t())}
             | {:ok, result :: term(), DomainEvent.t() | list(DomainEvent.t())}
             | {:error, term()})
  @type with_result :: with_result_fun() | {with_result_fun() | EventsToSync.context()}

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
  def execute({decision, context}, state) do
    case decision.(state) do
      {:error, error} -> {:error, Error.exception(error)}
      {:ok, event_or_events} -> {:ok, :ok, wrap(event_or_events, context)}
      {:ok, result, event_or_events} -> {:ok, {:ok, result}, wrap(event_or_events, context)}
      {result, event_or_events} -> {:ok, {:ok, result}, wrap(event_or_events, context)}
      nil_or_event_or_events -> {:ok, :ok, wrap(nil_or_event_or_events, context)}
    end
  end

  @spec execute(without_result_fun(), Fold.result()) ::
          {:ok, :ok, EventsToSync.t()}
          | {:error, Error.t()}
  @spec execute(with_result_fun(), Fold.result()) ::
          {:ok, {:ok, term()}, EventsToSync.t()}
          | {:error, Error.t()}
  def execute(decision, state) do
    execute({decision, %{}}, state)
  end

  defp wrap(nil_or_event_or_events, context) do
    nil_or_event_or_events
    |> List.wrap()
    |> EventsToSync.new(context)
  end
end
