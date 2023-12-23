defmodule Equinox.Store do
  alias Equinox.Stream.StreamName
  alias Equinox.Events.{TimelineEvent, EventData}

  @type t :: module()
  @type expected_version :: -1 | non_neg_integer()
  @type written_position :: non_neg_integer()

  @callback fetch_timeline_events(StreamName.t(), from_version :: expected_version()) ::
              Enumerable.t(TimelineEvent.t())
  @callback write_event_data(StreamName.t(), nonempty_list(EventData.t()), expected_version()) ::
              {:ok, new_version :: written_position()}
              | {:error, DuplicateMessageId.t() | StreamVersionConflict.t() | Exception.t()}

  defmodule DuplicateMessageId do
    defexception message: "Message with given ID already exists"
    @type t :: %__MODULE__{message: String.t()}
  end

  defmodule StreamVersionConflict do
    defexception message: "Wrong expected version"
    @type t :: %__MODULE__{message: String.t()}
  end
end
