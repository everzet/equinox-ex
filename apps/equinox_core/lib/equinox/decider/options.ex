defmodule Equinox.Decider.Options do
  alias Equinox.Decider.ResyncPolicy

  @opts NimbleOptions.new!(
          store: [
            type: {:or, [:atom, {:tuple, [:atom, :keyword_list]}]},
            required: true,
            doc: "Persistence module that implements `Equinox.Store` behaviour"
          ],
          resync: [
            type: {:struct, ResyncPolicy},
            default: ResyncPolicy.default(),
            doc:
              "Retry / Attempts policy used to define policy for retrying based on the conflicting state when there's an Append conflict"
          ],
          context: [
            type: :map,
            default: %{},
            doc: "Optional context to pass with events to `Equinox.Store.sync/4`"
          ]
        )

  @type t :: [o()]
  @type o :: unquote(NimbleOptions.option_typespec(@opts))

  def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
  def docs, do: NimbleOptions.docs(@opts)
  def keys, do: Keyword.keys(@opts.schema)
end
