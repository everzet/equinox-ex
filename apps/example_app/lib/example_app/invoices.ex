defmodule ExampleApp.Invoices do
  defmodule Stream do
    alias Equinox.Codec.{StreamId, StreamName}

    def category, do: "Invoice"
    def id(invoice_id), do: StreamId.encode(invoice_id)
    def name(invoice_id), do: StreamName.encode(category(), id(invoice_id))
  end

  defmodule Events do
    use Equinox.Codec.EventStructs

    defmodule InvoiceRaised do
      defstruct [:payer_id, :amount, :due_date]

      defimpl Upcast do
        def upcast(raised) do
          raised |> Map.update!(:due_date, &Date.from_iso8601!/1)
        end
      end
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

    @impl Equinox.Fold
    def initial, do: :not_raised
    @impl Equinox.Fold
    def fold(events, state), do: Enum.reduce(events, state, &evolve(&2, &1))

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

    def summary(state) do
      case state do
        :not_raised ->
          nil

        %Invoice{} = invoice ->
          %{
            payer_id: invoice.payer_id,
            amount: invoice.amount,
            finalized: invoice.status == :finalized
          }
      end
    end
  end

  defmodule Store do
    use Equinox.MessageDb.Store.Unoptimized,
      fetch_conn: ExampleApp.MessageDb,
      write_conn: ExampleApp.MessageDb,
      batch_size: 500,
      codec: Events,
      fold: Fold
  end

  alias ExampleApp.CustomValidators
  alias Equinox.{UUID, Decider}
  alias Ecto.Changeset

  def raise(invoice_id, params) do
    with changeset <- invoice_change(params),
         {:ok, invoice_id} <- UUID.parse(invoice_id),
         {:ok, data} <- Changeset.apply_action(changeset, :raise_invoice) do
      invoice_id
      |> resolve()
      |> Decider.transact(&Decide.raise_invoice(&1, data))
    end
  end

  def record_payment(invoice_id, params) do
    with changeset <- payment_change(params),
         {:ok, invoice_id} <- UUID.parse(invoice_id),
         {:ok, data} <- Changeset.apply_action(changeset, :record_payment) do
      invoice_id
      |> resolve()
      |> Decider.transact(&Decide.record_payment(&1, data))
    end
  end

  def finalize(invoice_id) do
    with {:ok, invoice_id} <- UUID.parse(invoice_id) do
      invoice_id
      |> resolve()
      |> Decider.transact(&Decide.finalize_invoice/1)
    end
  end

  def read_invoice(invoice_id) do
    with {:ok, invoice_id} <- UUID.parse(invoice_id) do
      invoice_id
      |> resolve()
      |> Decider.query(&Query.summary/1)
    end
  end

  def invoice_change(params) do
    %{payer_id: :string, amount: :float, due_date: :date}
    |> then(&Changeset.cast({%{}, &1}, params, Map.keys(&1)))
    |> Changeset.validate_required([:payer_id, :amount, :due_date])
    |> CustomValidators.validate_uuid(:payer_id)
  end

  def payment_change(params) do
    %{reference: :string, amount: :float}
    |> then(&Changeset.cast({%{}, &1}, params, Map.keys(&1)))
    |> Changeset.validate_required([:reference, :amount])
    |> CustomValidators.validate_uuid(:reference)
  end

  defp resolve(invoice_id) do
    invoice_id
    |> Stream.name()
    |> Decider.async(
      store: Store,
      lifetime: Decider.LifetimePolicy.max_inactivity(:timer.seconds(5)),
      registry: ExampleApp.InvoicesRegistry,
      supervisor: ExampleApp.InvoicesSupervisor
    )
  end
end
