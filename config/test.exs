import Config

config :example_app,
  web_server: false,
  db_connection: false

config :example_app, ExampleApp.Invoices,
  supervisor: false,
  registry: false,
  decider: [
    store:
      {Equinox.Store.MemoryStore,
       codec: ExampleApp.Invoices.Events, fold: ExampleApp.Invoices.Fold},
    supervisor: :disabled,
    registry: :disabled
  ]
