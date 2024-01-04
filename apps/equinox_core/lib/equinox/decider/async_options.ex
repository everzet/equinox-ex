defmodule Equinox.Decider.Async.Options do
  @opts NimbleOptions.new!(
          supervisor: [
            type: {:custom, __MODULE__, :validate_supervisor, []},
            required: true,
            doc: "Name of the DynamicSupervisor which should parent the decider process"
          ],
          registry: [
            type: {:custom, __MODULE__, :validate_registry, []},
            required: true,
            doc: "Name of the Registry (or :global) under which our decider should be listed"
          ],
          lifetime: [
            type: {:custom, __MODULE__, :validate_lifetime, []},
            required: true,
            doc: "Server lifetime spec module that implements `Equinox.Lifetime` behaviour"
          ]
        )

  @type t :: [o]
  @type o :: unquote(NimbleOptions.option_typespec(@opts))

  def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
  def docs, do: NimbleOptions.docs(@opts)
  def keys, do: Keyword.keys(@opts.schema)

  def validate_supervisor(supervisor) do
    case supervisor do
      :disabled ->
        {:ok, :disabled}

      supervisor when is_atom(supervisor) ->
        if supervisor |> GenServer.whereis() |> Process.alive?() do
          {:ok, supervisor}
        else
          {:error, "must be a running DynamicSupervisor, but got #{inspect(supervisor)}"}
        end

      _ ->
        {:error,
         "must be a running DynamicSupervisor or :disabled, but got #{inspect(supervisor)}"}
    end
  end

  def validate_registry(registry) do
    case registry do
      :disabled ->
        {:ok, :disabled}

      :global ->
        {:ok, :global}

      {:global, prefix} ->
        {:ok, {:global, prefix}}

      registry when is_atom(registry) ->
        if registry |> GenServer.whereis() |> Process.alive?() do
          {:ok, registry}
        else
          {:error, "must be a running Registry, but got #{inspect(registry)}"}
        end

      _ ->
        {:error,
         "must be a running Registry, :global, {:global, prefix} or :disabled, but got #{inspect(registry)}"}
    end
  end

  def validate_lifetime(lifetime) do
    case lifetime do
      lifetime when is_atom(lifetime) ->
        Code.ensure_loaded(lifetime)

        if function_exported?(lifetime, :after_init, 1) do
          {:ok, lifetime}
        else
          {:error,
           "must be a module implementing `Equinox.Lifetime`, but got #{inspect(lifetime)}"}
        end

      _ ->
        {:error, "must be a module implementing `Equinox.Lifetime`, but got #{inspect(lifetime)}"}
    end
  end
end
