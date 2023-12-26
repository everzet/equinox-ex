defmodule ExampleApp.Invoices do
  defmodule Stream do
    alias Equinox.Stream.{Category, StreamId, StreamName}

    def category, do: Category.new("Invoice")
    def id(invoice_id), do: StreamId.new([String.downcase(invoice_id)])
    def name(invoice_id), do: StreamName.new(category(), id(invoice_id))
  end

  defmodule Events do
    use Equinox.Codec.EventStructs, structs_mod: __MODULE__

    defmodule InvoiceRaised do
      defstruct [:payer_id, :amount, :due_date]
    end

    defmodule PaymentReceived do
      defstruct [:reference, :amount]
    end

    defmodule InvoiceFinalized do
      defstruct []
    end
  end

  defmodule Fold do
    @behaviour Equinox.Fold
    alias Events.{InvoiceRaised, PaymentReceived, InvoiceFinalized}

    defmodule Invoice do
      @enforce_keys [:payer_id, :amount]
      defstruct status: :raised,
                payer_id: nil,
                amount: nil,
                paid: 0,
                payments: MapSet.new()

      def raise(payer_id, amount), do: %__MODULE__{payer_id: payer_id, amount: amount}
      def raised_with?(fst, snd), do: fst.payer_id == snd.payer_id and fst.amount == snd.amount
      def receive_payment(invoice, ref), do: update_in(invoice.payments, &MapSet.put(&1, ref))
      def payment_received?(invoice, ref), do: MapSet.member?(invoice.payments, ref)
      def pay_amount(invoice, amount), do: update_in(invoice.paid, &(&1 + amount))
      def finalize(invoice), do: put_in(invoice.status, :finalized)
    end

    def initial, do: :not_raised

    def evolve(:not_raised, %InvoiceRaised{} = raised) do
      Invoice.raise(raised.payer_id, raised.amount)
    end

    def evolve(%Invoice{status: :raised} = invoice, %PaymentReceived{} = paid) do
      invoice
      |> Invoice.receive_payment(paid.reference)
      |> Invoice.pay_amount(paid.amount)
    end

    def evolve(%Invoice{status: :raised} = invoice, %InvoiceFinalized{}) do
      Invoice.finalize(invoice)
    end
  end

  defmodule Decide do
    alias Fold.Invoice
    alias Events.{InvoiceRaised, PaymentReceived, InvoiceFinalized}

    def raise_invoice(state, data) do
      case state do
        :not_raised ->
          struct!(InvoiceRaised, data)

        %Invoice{status: :finalized} ->
          {:error, :invoice_finalized}

        %Invoice{status: :raised} = invoice ->
          if not Invoice.raised_with?(invoice, data) do
            {:error, :invoice_already_raised}
          end
      end
    end

    def record_payment(state, data) do
      case state do
        :not_raised ->
          {:error, :invoice_not_raised}

        %Invoice{status: :finalized} ->
          {:error, :invoice_finalized}

        %Invoice{status: :raised} = invoice ->
          if not Invoice.payment_received?(invoice, data.reference) do
            struct!(PaymentReceived, data)
          end
      end
    end

    def finalize_invoice(state) do
      case state do
        :not_raised -> {:error, :invoice_not_raised}
        %Invoice{status: :raised} -> %InvoiceFinalized{}
        %Invoice{status: :finalized} -> nil
      end
    end
  end

  defmodule Query do
    alias Fold.Invoice

    def summary(:not_raised), do: nil

    def summary(%Invoice{} = invoice) do
      %{
        payer_id: invoice.payer_id,
        amount: invoice.amount,
        finalized: invoice.status == :finalized
      }
    end
  end

  alias Ecto.Changeset
  alias Equinox.Decider
  alias ExampleApp.Validator

  def raise(invoice_id, params) do
    with {:ok, data} <- Validator.validate(params, &invoice_changeset/1) do
      invoice_id
      |> resolve()
      |> Decider.transact(&Decide.raise_invoice(&1, data))
    end
  end

  def record_payment(invoice_id, params) do
    with {:ok, data} <- Validator.validate(params, &payment_changeset/1) do
      invoice_id
      |> resolve()
      |> Decider.transact(&Decide.record_payment(&1, data))
    end
  end

  def finalize(invoice_id) do
    invoice_id
    |> resolve()
    |> Decider.transact(&Decide.finalize_invoice/1)
  end

  def read_invoice(invoice_id) do
    invoice_id
    |> resolve()
    |> Decider.query(&Query.summary/1)
  end

  defp invoice_changeset(params) do
    types = %{payer_id: :string, amount: :float, due_date: :date}

    {%{}, types}
    |> Changeset.cast(params, Map.keys(types))
    |> Changeset.validate_required([:payer_id, :amount, :due_date])
    |> Validator.validate_uuid(:payer_id)
  end

  defp payment_changeset(params) do
    types = %{reference: :string, amount: :float}

    {%{}, types}
    |> Changeset.cast(params, Map.keys(types))
    |> Changeset.validate_required([:reference, :amount])
    |> Validator.validate_uuid(:reference)
  end

  defp resolve(invoice_id) do
    invoice_id
    |> Stream.name()
    |> Decider.Stateful.for_stream(
      supervisor: ExampleApp.InvoicesSupervisor,
      registry: ExampleApp.InvoicesRegistry,
      lifetime: Equinox.Lifetime.StayAliveFor30Seconds,
      store: ExampleApp.EventStore.Unoptimized,
      codec: Events,
      fold: Fold
    )
  end
end
