defmodule ExampleAppHttp.App do
  use Plug.Router
  use Plug.ErrorHandler

  plug(:match)
  plug(Plug.Parsers, parsers: [:json], pass: ["application/json"], json_decoder: Jason)
  plug(:dispatch)

  alias Ecto.UUID
  alias Ecto.Changeset
  alias ExampleApp.{Payers, Invoices}

  get "/payers/:payer_id" do
    case Payers.read_profile(payer_id) do
      {nil, _} -> send_resp(conn, 404, "not found")
      {profile, _} -> json_resp(conn, 200, profile)
    end
  end

  put "/payers/:payer_id" do
    case Payers.update_profile(payer_id, conn.body_params) do
      {:ok, _} -> send_resp(conn, 204, "ok")
      {:error, %Changeset{errors: errors}} -> json_resp(conn, 400, %{error: inspect(errors)})
      {:error, term} -> json_resp(conn, 400, %{error: inspect(term)})
    end
  end

  delete "/payers/:payer_id" do
    case Payers.delete_payer(payer_id) do
      {:ok, _} -> send_resp(conn, 204, "ok")
      {:error, term} -> json_resp(conn, 400, %{error: inspect(term)})
    end
  end

  post "/invoices" do
    invoice_id = UUID.generate()

    case Invoices.raise(invoice_id, conn.body_params) do
      {:ok, _} -> json_resp(conn, 201, %{id: invoice_id})
      {:error, %Changeset{errors: errors}} -> json_resp(conn, 400, %{error: inspect(errors)})
      {:error, term} -> json_resp(conn, 400, %{error: inspect(term)})
    end
  end

  get "/invoices/:invoice_id" do
    case Invoices.read_invoice(invoice_id) do
      {nil, _} -> send_resp(conn, 404, "not found")
      {invoice, _} -> json_resp(conn, 200, invoice)
    end
  end

  match _ do
    send_resp(conn, 404, "route not found")
  end

  defp json_resp(conn, status, map), do: send_resp(conn, status, Jason.encode!(map))
end
