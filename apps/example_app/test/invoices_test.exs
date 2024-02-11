defmodule ExampleApp.InvoicesTest do
  use ExUnit.Case, async: true

  alias Equinox.UUID
  alias Equinox.Store.MemoryStore
  alias ExampleApp.Invoices

  setup do
    MemoryStore.checkout()
    MemoryStore.register_listener(self())
  end

  @valid_params %{
    "payer_id" => UUID.generate(),
    "amount" => 23.50,
    "due_date" => ~D[2024-02-03]
  }

  test "raising invoice" do
    assert :ok = Invoices.raise(UUID.generate(), @valid_params)
    assert_receive %{type: "InvoiceRaised", data: @valid_params}
  end
end
