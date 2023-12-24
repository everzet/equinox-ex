defmodule Equinox.State do
  alias Equinox.{Store, Codec, Fold}

  @enforce_keys [:value, :version]
  defstruct [:value, :version]

  @type t :: %__MODULE__{value: value(), version: version()}
  @type value :: any()
  @type version :: Store.stream_version()

  @type fetch_function! :: (-> Enumerable.t(TimelineEvent.t()))
  @type write_function! :: (Enumerable.t(EventData.t()) -> Store.written_position())

  @spec init(Fold.t()) :: t()
  def init(fold), do: %__MODULE__{value: fold.initial(), version: -1}

  @spec load!(t(), Codec.t(), Fold.t(), fetch_function!()) :: t()
  def load!(%__MODULE__{} = state, codec, fold, fetch_fun) do
    fetch_fun.()
    |> Codec.decode_all!(codec)
    |> Fold.fold(state, fold)
  end

  @spec sync!(t(), list(DomainEvent.t()), Codec.ctx(), Codec.t(), Fold.t(), write_function!()) ::
          t()
  def sync!(%__MODULE__{} = state, domain_events, ctx, codec, fold, write_fun) do
    new_version =
      domain_events
      |> Codec.encode_all!(ctx, codec)
      |> write_fun.()

    domain_events
    |> Enum.zip((state.version + 1)..new_version)
    |> Fold.fold(state, fold)
  end
end
