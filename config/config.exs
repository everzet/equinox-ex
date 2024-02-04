# This file is responsible for configuring your umbrella
# and **all applications** and their dependencies with the
# help of the Config module.
#
# Note that all applications in your umbrella share the
# same configuration and dependencies, which is why they
# all use the same configuration file. If you want different
# configurations or dependencies per app, it is best to
# move said applications out of the umbrella.
import Config

# Sample configuration:
#
#     config :logger, :console,
#       level: :info,
#       format: "$date $time [$level] $metadata$message\n",
#       metadata: [:user_id]
#

config :example_app, ExampleApp.Payers,
  supervisor: [
    name: ExampleApp.Payers.Supervisor,
    strategy: :one_for_one
  ],
  cache: [
    name: ExampleApp.Payers.Cache,
    max_memory: {50, :mb}
  ],
  decider: [
    load: :any_cached_value,
    store:
      {Equinox.MessageDb.Store.LatestKnownEvent,
       conn: ExampleApp.MessageDbConn,
       cache: {Equinox.Cache.LRU, name: ExampleApp.Payers.Cache},
       codec: ExampleApp.Payers.Events,
       fold: ExampleApp.Payers.Fold},
    supervisor: ExampleApp.Payers.Supervisor,
    registry: :global
  ]

config :example_app, ExampleApp.Invoices,
  supervisor: [
    name: ExampleApp.Invoices.Supervisor,
    strategy: :one_for_one
  ],
  registry: [
    name: ExampleApp.Invoices.Registry,
    keys: :unique
  ],
  decider: [
    store:
      {Equinox.MessageDb.Store.LatestKnownEvent,
       conn: ExampleApp.MessageDbConn,
       codec: ExampleApp.Invoices.Events,
       fold: ExampleApp.Invoices.Fold},
    supervisor: ExampleApp.Invoices.Supervisor,
    registry: ExampleApp.Invoices.Registry,
    lifetime: [max_inactivity: :timer.seconds(1)]
  ]

import_config "#{config_env()}.exs"
