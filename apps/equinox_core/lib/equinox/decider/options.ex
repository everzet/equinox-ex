defmodule Equinox.Decider.Options do
  @opts NimbleOptions.new!(
          store: [
            type: {:custom, __MODULE__, :validate_store, []},
            required: true,
            doc: "Persistence module that implements `Equinox.Store` behaviour"
          ],
          codec: [
            type: {:custom, __MODULE__, :validate_codec, []},
            required: true,
            doc: "Event (en|de)coding module that implements `Equinox.Codec` behaviour"
          ],
          fold: [
            type: {:custom, __MODULE__, :validate_fold, []},
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

  def validate_store(store) do
    case store do
      store when is_atom(store) ->
        Code.ensure_loaded(store)

        if function_exported?(store, :load!, 4) do
          {:ok, store}
        else
          {:error, "must be a module implementing `Equinox.Store`, but got #{inspect(store)}"}
        end

      _ ->
        {:error, "must be a module implementing `Equinox.Store`, but got #{inspect(store)}"}
    end
  end

  def validate_codec(codec) do
    case codec do
      codec when is_atom(codec) ->
        Code.ensure_loaded(codec)

        if function_exported?(codec, :encode, 2) do
          {:ok, codec}
        else
          {:error, "must be a module implementing `Equinox.Codec`, but got #{inspect(codec)}"}
        end

      _ ->
        {:error, "must be a module implementing `Equinox.Codec`, but got #{inspect(codec)}"}
    end
  end

  def validate_fold(fold) do
    case fold do
      fold when is_atom(fold) ->
        Code.ensure_loaded(fold)

        if function_exported?(fold, :evolve, 2) do
          {:ok, fold}
        else
          {:error, "must be a module implementing `Equinox.Fold`, but got #{inspect(fold)}"}
        end

      _ ->
        {:error, "must be a module implementing `Equinox.Fold`, but got #{inspect(fold)}"}
    end
  end
end
