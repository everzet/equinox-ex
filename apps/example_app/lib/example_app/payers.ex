defmodule ExampleApp.Payers do
  alias ExampleApp.Payers.Events.PayerDeleted

  defmodule Stream do
    alias Equinox.Stream.{Category, StreamId, StreamName}

    def category, do: Category.new("Payer")
    def id(payer_id), do: StreamId.new([String.downcase(payer_id)])
    def name(payer_id), do: StreamName.new(category(), id(payer_id))
  end

  defmodule Events do
    use Equinox.Codec.EventStructs, structs_mod: __MODULE__

    defmodule PayerProfileUpdated do
      defstruct [:name, :email]
    end

    defmodule PayerDeleted do
      defstruct []
    end
  end

  defmodule Fold do
    @behaviour Equinox.Fold
    alias Events.{PayerProfileUpdated, PayerDeleted}

    def initial, do: nil
    def evolve(_, %PayerProfileUpdated{} = updated), do: Map.from_struct(updated)
    def evolve(_, %PayerDeleted{}), do: nil
  end

  defmodule Decide do
    alias Events.{PayerProfileUpdated, PayerDeleted}

    def update_profile(state, data) do
      case state do
        nil ->
          struct!(PayerProfileUpdated, data)

        map ->
          if not Map.equal?(map, data) do
            struct!(PayerProfileUpdated, data)
          end
      end
    end

    def delete_payer(nil), do: nil
    def delete_payer(_), do: %PayerDeleted{}
  end

  alias Ecto.Changeset
  alias Equinox.Decider
  alias ExampleApp.Validator

  def update_profile(payer_id, params) do
    with {:ok, data} <- Validator.validate(params, &payer_changeset/1) do
      payer_id
      |> resolve()
      |> Decider.transact(&Decide.update_profile(&1, data))
    end
  end

  def delete_payer(payer_id) do
    payer_id
    |> resolve()
    |> Decider.transact(&Decide.delete_payer/1)
  end

  def read_profile(payer_id) do
    payer_id
    |> resolve()
    |> Decider.query(& &1)
  end

  defp payer_changeset(params) do
    types = %{name: :string, email: :string}

    {%{}, types}
    |> Changeset.cast(params, Map.keys(types))
    |> Changeset.validate_required([:name, :email])
    |> Validator.validate_email(:email)
  end

  defp resolve(payer_id) do
    payer_id
    |> Stream.name()
    |> Decider.Stateful.for_stream(
      supervisor: ExampleApp.PayersSupervisor,
      registry: :global,
      lifetime: Equinox.Lifetime.StayAliveFor30Seconds,
      store: ExampleApp.EventStore.LatestKnownEvent,
      codec: Events,
      fold: Fold
    )
  end
end
