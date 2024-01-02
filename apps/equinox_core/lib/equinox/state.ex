defmodule Equinox.State do
  alias Equinox.Events.{TimelineEvent, EventData, DomainEvent}
  alias Equinox.{Store, Codec, Fold}

  @enforce_keys [:value, :version]
  defstruct [:value, :version]

  @type t :: %__MODULE__{value: value(), version: Store.stream_version()}
  @type value :: any()

  @spec new(value(), Store.stream_version()) :: t()
  def new(value, version), do: %__MODULE__{value: value, version: version}

  @spec init(Fold.t()) :: t()
  def init(fold), do: new(fold.initial(), -1)

  @spec update(t(), (value() -> value()), Store.stream_version()) :: t()
  def update(%__MODULE__{value: value}, fun, new_version), do: new(fun.(value), new_version)

  @type fetcher :: (-> Enumerable.t(TimelineEvent.t()))
  @spec load!(t(), Codec.t(), Fold.t(), fetcher) :: t()
  def load!(%__MODULE__{} = state, codec, fold, fetch_fun) do
    fetch_fun.()
    |> Codec.decode_stream!(codec)
    |> Fold.fold(state, fold)
  end

  @type writer :: (list(EventData.t()) -> Store.stream_version())
  @spec sync!(t(), list(DomainEvent.t()), Codec.context(), Codec.t(), Fold.t(), writer) :: t()
  def sync!(%__MODULE__{} = state, domain_events, context, codec, fold, write_fun) do
    written_version =
      domain_events
      |> Codec.encode_list!(context, codec)
      |> write_fun.()

    new_state =
      domain_events
      |> Enum.zip((state.version + 1)..written_version)
      |> Fold.fold(state, fold)

    new_state
  end
end
