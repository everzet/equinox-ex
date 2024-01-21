defmodule ExampleApp.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      [
        ExampleApp.Payers,
        ExampleApp.Invoices
      ]
      |> toggle_db_connection()
      |> toggle_web_server()

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: ExampleApp.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp toggle_web_server(children) do
    if Application.fetch_env!(:example_app, :web_server) do
      # append Bandit at the end as it should only launch when everything else is up
      children ++ [{Bandit, plug: ExampleAppHttp.App, port: 6789}]
    else
      children
    end
  end

  defp toggle_db_connection(children) do
    if Application.fetch_env!(:example_app, :web_server) do
      # prepend DB connection at the beginning as it is core dependency for other services
      [{Equinox.MessageDb.Connection, name: ExampleApp.MessageDbConn, pool_size: 20} | children]
    else
      children
    end
  end
end
