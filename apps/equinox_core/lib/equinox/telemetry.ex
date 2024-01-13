defmodule Equinox.Telemetry do
  @moduledoc """
  Equinox telemetry spans.
  """

  def span_decider_query(decider, query, fun) do
    meta = %{original_decider: decider, query_fun: query}

    :telemetry.span([:equinox, :decider, :query], meta, fn ->
      {result, loaded} = fun.()
      {{result, loaded}, Map.put(meta, :loaded_decider, loaded)}
    end)
  end

  def span_decider_transact(decider, decision, context, fun) do
    meta = %{original_decider: decider, decision_fun: decision, context: context}

    :telemetry.span([:equinox, :decider, :transact], meta, fn ->
      case fun.() do
        {:ok, decider} ->
          {{:ok, decider}, Map.put(meta, :synced_decider, decider)}

        {:error, error, loaded} ->
          {{:error, error, loaded},
           meta |> Map.put(:loaded_decider, loaded) |> Map.put(:error, error)}
      end
    end)
  end

  def span_transact_decision(decider, decision, context, attempt, fun) do
    meta = %{decider: decider, decision_fun: decision, context: context, attempt: attempt}

    :telemetry.span([:equinox, :decider, :transact, :decision], meta, fn ->
      case fun.() do
        {:ok, outcome} -> {{:ok, outcome}, Map.put(meta, :outcome, outcome)}
        {:error, error} -> {{:error, error}, Map.put(meta, :error, error)}
      end
    end)
  end

  def span_transact_resync(decider, attempt, fun) do
    meta = %{original_decider: decider, attempt: attempt}

    :telemetry.span([:equinox, :decider, :transact, :resync], meta, fn ->
      synced = fun.()
      {synced, Map.put(meta, :synced_decider, synced)}
    end)
  end

  def span_load_state(decider, attempt, fun) do
    meta = %{decider: decider, attempt: attempt}

    :telemetry.span([:equinox, :decider, :load], meta, fn ->
      case fun.() do
        {:ok, loaded} ->
          {{:ok, loaded}, Map.put(meta, :loaded_state, loaded)}

        {:error, error, partial} ->
          {{:error, error, partial},
           meta |> Map.put(:partial_state, partial) |> Map.put(:error, error)}
      end
    end)
  end

  def span_sync_state(decider, outcome, attempt, fun) do
    meta = %{decider: decider, outcome: outcome, attempt: attempt}

    :telemetry.span([:equinox, :decider, :sync], meta, fn ->
      case fun.() do
        {:ok, synced} -> {{:ok, synced}, Map.put(meta, :synced_state, synced)}
        {:error, error} -> {{:error, error}, Map.put(meta, :error, error)}
      end
    end)
  end

  def async_server_init(server_state) do
    :telemetry.execute(
      [:equinox, :decider, :async, :init],
      %{system_time: System.system_time()},
      server_state
    )
  end

  def async_server_stop(server_state, reason) do
    :telemetry.execute(
      [:equinox, :decider, :async, :stop],
      %{system_time: System.system_time()},
      Map.put(server_state, :reason, reason)
    )
  end

  def span_async_load(server_state, init_fun) do
    :telemetry.span([:equinox, :decider, :async, :load], server_state, fn ->
      decider = init_fun.()
      {decider, Map.put(server_state, :decider, decider)}
    end)
  end

  def span_async_query(server_state, query, fun) do
    meta = Map.put(server_state, :query_fun, query)

    :telemetry.span([:equinox, :decider, :async, :query], meta, fn ->
      {result, loaded} = fun.()
      {{result, loaded}, Map.put(meta, :result, result)}
    end)
  end

  def span_async_transact(server_state, decision, context, fun) do
    meta = %{
      settings: server_state.settings,
      original_decider: server_state.decider,
      decision_fun: decision,
      context: context
    }

    :telemetry.span([:equinox, :decider, :async, :transact], meta, fn ->
      case fun.() do
        {:ok, decider} ->
          {{:ok, decider}, Map.put(meta, :synced_decider, decider)}

        {:error, error, loaded} ->
          {{:error, error, loaded},
           meta |> Map.put(:loaded_decider, loaded) |> Map.put(:error, error)}
      end
    end)
  end
end
