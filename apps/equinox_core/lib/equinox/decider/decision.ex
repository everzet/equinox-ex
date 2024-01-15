defmodule Equinox.Decider.Decision do
  alias Equinox.Fold
  alias Equinox.Events.DomainEvent

  @type without_result ::
          (Fold.result() ->
             nil
             | DomainEvent.t()
             | list(DomainEvent.t())
             | {:ok, DomainEvent.t() | list(DomainEvent.t())}
             | {:error, term()})

  @type with_result ::
          (Fold.result() ->
             {result :: term(), DomainEvent.t() | list(DomainEvent.t())}
             | {:ok, result :: term(), DomainEvent.t() | list(DomainEvent.t())}
             | {:error, term()})

  @type t :: without_result() | with_result()

  defmodule Error do
    defexception [:message, :term]
    @type t :: %__MODULE__{message: String.t(), term: term()}

    def exception(error_term) do
      %__MODULE__{message: "Decision error: #{inspect(error_term)}", term: error_term}
    end
  end

  @spec execute(without_result(), Fold.result()) ::
          {:ok, list(DomainEvent.t())} | {:error, Error.t()}
  @spec execute(with_result(), Fold.result()) ::
          {:ok, term(), list(DomainEvent.t())} | {:error, Error.t()}
  def execute(decision, state) do
    case decision.(state) do
      {:error, error} -> {:error, Error.exception(error)}
      {:ok, event_or_events} -> {:ok, List.wrap(event_or_events)}
      {:ok, result, event_or_events} -> {:ok, result, List.wrap(event_or_events)}
      {result, event_or_events} -> {:ok, result, List.wrap(event_or_events)}
      nil_or_event_or_events -> {:ok, List.wrap(nil_or_event_or_events)}
    end
  end
end
