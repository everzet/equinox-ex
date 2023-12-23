defmodule Equinox.Store do
  alias Equinox.Stream.StreamName
  alias Equinox.Events.{TimelineEvent, EventData}

  @type t :: module()
  @type stream_version :: -1 | non_neg_integer()
  @type written_position :: non_neg_integer()

  @type read :: (-> Enumerable.t(TimelineEvent.t()))
  @type write :: (Enumerable.t(EventData.t()) -> written_position())

  @callback fetch_timeline_events(StreamName.t(), from_version :: stream_version()) ::
              Enumerable.t(TimelineEvent.t())
  @callback write_event_data(StreamName.t(), nonempty_list(EventData.t()), stream_version()) ::
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
