defmodule ExampleApp.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DynamicSupervisor, name: ExampleApp.Payers.Supervisor, strategy: :one_for_one},
      {Equinox.Cache.LRU, name: ExampleApp.Payers.Cache, max_size: 100_000, max_memory: 10_000},
      {DynamicSupervisor, name: ExampleApp.Invoices.Supervisor, strategy: :one_for_one},
      {Registry, name: ExampleApp.Invoices.Registry, keys: :unique}
    ]

    children =
      if Mix.env() != :test do
        children ++
          [
            {Equinox.MessageDb.Connection, name: ExampleApp.MessageDb, pool_size: 20},
            {Bandit, plug: ExampleAppHttp.App, port: 6789}
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
