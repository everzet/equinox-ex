defmodule Equinox.Fold do
  alias Equinox.Events.DomainEvent

  @type t :: module()
  @type state :: any()
  @type versioned_state :: {state(), non_neg_integer()}

  @callback initial() :: state()
  @callback evolve(state(), DomainEvent.t()) :: state()

  defmodule FoldError do
    @enforce_keys [:message]
    defexception [:message, :exception]
    @type t :: %__MODULE__{message: String.t(), exception: nil | Exception.t()}
  end

  @spec fold_versioned(t(), state(), Enumerable.t(DomainEvent.indexed())) :: versioned_state()
  def fold_versioned(fold, state, domain_events) do
    Enum.reduce(domain_events, {state, -1}, fn {event, position}, {state, _} ->
      try do
        {fold.evolve(state, event), position}
      rescue
        exception ->
          reraise FoldError,
                  [
                    message: "#{inspect(fold)}.evolve: #{inspect(exception)}",
                    exception: exception
                  ],
                  __STACKTRACE__
      end
    end)
  end
end
