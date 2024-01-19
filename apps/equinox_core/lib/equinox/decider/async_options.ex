defmodule Equinox.Decider.Async.Options do
  alias Equinox.Decider.LifetimePolicy

  @opts NimbleOptions.new!(
          supervisor: [
            type: {:or, [:atom, {:in, [:disabled]}]},
            required: true,
            doc: "Name of the DynamicSupervisor which should parent the decider process"
          ],
          registry: [
            type:
              {:or,
               [
                 :atom,
                 {:in, [:global]},
                 {:tuple, [{:in, [:global]}, :string]},
                 {:in, [:disabled]}
               ]},
            required: true,
            doc: "Name of the Registry (or :global) under which our decider should be listed"
          ],
          lifetime: [
            type: {:struct, LifetimePolicy},
            default: LifetimePolicy.default(),
            doc: "Decider server lifetime policy"
          ],
          context: [
            type: :map,
            default: %{},
            doc: "Optional context to pass with events to `Equinox.Store.sync/4`"
          ]
        )

  @type t :: [o]
  @type o :: unquote(NimbleOptions.option_typespec(@opts))

  def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
  def docs, do: NimbleOptions.docs(@opts)
  def keys, do: Keyword.keys(@opts.schema)
end
