defmodule Equinox.Store do
  alias Equinox.Stream.StreamName
  alias Equinox.Events.{TimelineEvent, EventData}

  @type expected_version :: -1 | non_neg_integer()
  @type written_position :: non_neg_integer()

  @type fetcher :: (StreamName.t() -> Enumerable.t(TimelineEvent.t()))
  @type writer ::
          (StreamName.t(), list(EventData.t()), expected_version() ->
             {:ok, new_version :: written_position()}
             | {:error, DuplicateMessageId.t() | StreamVersionConflict.t() | Exception.t()})

  defmodule DuplicateMessageId do
    defexception message: "Message with given ID already exists"
    @type t :: %__MODULE__{}
  end

  defmodule StreamVersionConflict do
    defexception message: "Wrong expected version"
    @type t :: %__MODULE__{}
  end
end
