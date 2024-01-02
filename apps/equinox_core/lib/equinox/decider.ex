defmodule Equinox.Decider do
  alias Equinox.Decider.{Stateless, Stateful}
  alias Equinox.Decider.Actions.{Query, Decision}

  @type context :: map()

  @spec stateless(String.t(), Stateless.Options.t()) :: Stateless.t()
  def stateless(stream_name, opts) when is_bitstring(stream_name) do
    Stateless.for_stream(stream_name, opts)
  end

  @spec stateful(String.t(), [Stateless.Options.o() | Stateful.Options.o()]) :: Stateful.t()
  def stateful(stream_name, opts) when is_bitstring(stream_name) do
    {stateful_opts, stateless_opts} = Keyword.split(opts, [:supervisor, :registry, :lifetime])

    stream_name
    |> stateless(stateless_opts)
    |> stateful(stateful_opts)
  end

  @spec stateful(Stateless.t(), Stateful.Options.t()) :: Stateful.t()
  def stateful(%Stateless{} = stateless, opts), do: Stateful.wrap_stateless(stateless, opts)

  @spec load(Stateless.t()) :: Stateless.t()
  def load(%Stateless{} = stateless), do: Stateless.load(stateless)

  @spec load(String.t(), Stateless.Options.t()) :: Stateless.t()
  def load(stream_name, opts) when is_bitstring(stream_name) do
    stream_name
    |> stateless(opts)
    |> load()
  end

  @spec start(Stateful.t()) :: Stateful.t() | pid()
  def start(%Stateful{} = stateful), do: Stateful.start(stateful)

  @spec start(String.t(), [Stateless.Options.o() | Stateful.Options.o()]) :: Stateful.t() | pid()
  @spec start(Stateless.t(), Stateful.Options.t()) :: Stateful.t() | pid()
  def start(stream_name_or_stateless, stateful_or_both_opts) do
    stream_name_or_stateless
    |> stateful(stateful_or_both_opts)
    |> start()
  end

  @spec query(pid(), Query.t()) :: any()
  @spec query(Stateful.t(), Query.t()) :: any()
  @spec query(Stateless.t(), Query.t()) :: any()
  def query(decider, query) do
    case decider do
      pid when is_pid(pid) -> Stateful.query(pid, query)
      %Stateful{} = decider -> Stateful.query(decider, query)
      %Stateless{} = decider -> Stateless.query(decider, query)
    end
  end

  @spec transact(pid(), Decision.t(), context()) ::
          {:ok, pid()} | {:error, term()}
  @spec transact(Stateful.t(), Decision.t(), context()) ::
          {:ok, Stateful.t()} | {:error, term()}
  @spec transact(Stateless.t(), Decision.t(), context()) ::
          {:ok, Stateless.t()} | {:error, term()}
  def transact(decider, decision, context \\ %{}) do
    case decider do
      pid when is_pid(pid) -> Stateful.transact(pid, decision, context)
      %Stateful{} = decider -> Stateful.transact(decider, decision, context)
      %Stateless{} = decider -> Stateless.transact(decider, decision, context)
    end
  end
end
