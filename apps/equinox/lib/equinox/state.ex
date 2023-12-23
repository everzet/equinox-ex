defmodule Equinox.State do
  alias Equinox.{Store, Codec, Fold}

  @enforce_keys [:value, :version]
  defstruct [:value, :version]

  @type t :: %__MODULE__{value: value(), version: version()}
  @type value :: any()
  @type version :: Store.stream_version()

  @spec new(value()) :: t()
  def new(value), do: %__MODULE__{value: value, version: -1}

  @spec load(t(), Codec.t(), Fold.t(), Store.read()) :: t()
  def load(%__MODULE__{} = state, codec, fold, read_fun) do
    read_fun.()
    |> then(&Codec.decode_timeline_events_with_indexes(codec, &1))
    |> then(&Fold.fold(fold, state, &1))
  end

  @spec sync(t(), Codec.ctx(), Codec.t(), Fold.t(), list(DomainEvent.t()), Store.write()) :: t()
  def sync(%__MODULE__{} = state, context, codec, fold, domain_events, write_fun) do
    new_version =
      domain_events
      |> then(&Codec.encode_domain_events(codec, context, &1))
      |> write_fun.()

    domain_events
    |> Enum.zip((state.version + 1)..new_version)
    |> then(&Fold.fold(fold, state, &1))
  end
end
