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

  @spec load!(
          t(),
          Codec.t(),
          Fold.t(),
          (-> Enumerable.t(TimelineEvent.t()))
        ) :: t()
  def load!(%__MODULE__{} = state, codec, fold, fetch_fun) do
    fetch_fun.()
    |> Codec.decode_with_position!(codec)
    |> Fold.fold(state, fold)
  end

  @spec sync!(
          t(),
          list(DomainEvent.t()),
          Codec.ctx(),
          Codec.t(),
          Fold.t(),
          (list(EventData.t()) -> Store.stream_version())
        ) :: t()
  def sync!(%__MODULE__{} = state, domain_events, ctx, codec, fold, write_fun) do
    new_version =
      domain_events
      |> Codec.encode!(ctx, codec)
      |> write_fun.()

    domain_events
    |> Enum.zip((state.version + 1)..new_version)
    |> Fold.fold(state, fold)
  end
end
