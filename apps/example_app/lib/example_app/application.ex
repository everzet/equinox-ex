defmodule ExampleApp.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      ExampleApp.Payers,
      ExampleApp.Invoices
    ]

    children =
      if Mix.env() != :test do
        [
          {Equinox.MessageDb.Connection, name: ExampleApp.MessageDbConn, pool_size: 20},
          {Bandit, plug: ExampleAppHttp.App, port: 6789}
          | children
        ]
      else
        children
      end

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: ExampleApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
