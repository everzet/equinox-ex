defmodule Equinox.Decider.Decision do
  alias Equinox.Fold
  alias Equinox.Events.DomainEvent

  @type t ::
          (Fold.result() ->
             nil
             | DomainEvent.t()
             | list(DomainEvent.t())
             | {:ok, DomainEvent.t() | list(DomainEvent.t())}
             | {:error, term()})

  defmodule Error do
    defexception [:message, :term]
    @type t :: %__MODULE__{message: String.t(), term: term()}

    def exception(error_term) do
      %__MODULE__{message: "Decision error: #{inspect(error_term)}", term: error_term}
    end
  end

  @spec execute(t(), Fold.result()) :: {:ok, list(DomainEvent.t())} | {:error, Error.t()}
  def execute(decision, state) do
    case decision.(state) do
      {:error, error} -> {:error, Error.exception(error)}
      {:ok, event_or_events} -> {:ok, List.wrap(event_or_events)}
      nil_or_event_or_events -> {:ok, List.wrap(nil_or_event_or_events)}
    end
  end
end
