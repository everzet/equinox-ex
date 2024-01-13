defmodule Equinox.Decider.Options do
  @opts NimbleOptions.new!(
          store: [
            type: :atom,
            required: true,
            doc: "Persistence module that implements `Equinox.Store` behaviour"
          ],
          codec: [
            type: :atom,
            required: true,
            doc: "Event (en|de)coding module that implements `Equinox.Codec` behaviour"
          ],
          fold: [
            type: :atom,
            required: true,
            doc: "State generation module that implements `Equinox.Fold` behaviour"
          ],
          context: [
            type: :map,
            default: %{},
            doc: "Decider-wide context. Merged with context passed explicitly via transact"
          ],
          max_load_attempts: [
            type: :pos_integer,
            default: 2,
            doc: "How many times (in total) should we try to load the state on load errors"
          ],
          max_sync_attempts: [
            type: :pos_integer,
            default: 2,
            doc: "How many times (in total) should we try to sync the state on write errors"
          ],
          max_resync_attempts: [
            type: :non_neg_integer,
            default: 1,
            doc: "How many times should we try to resync the state on version conflict"
          ]
        )

  @type t :: [o()]
  @type o :: unquote(NimbleOptions.option_typespec(@opts))

  def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
  def docs, do: NimbleOptions.docs(@opts)
  def keys, do: Keyword.keys(@opts.schema)
end
