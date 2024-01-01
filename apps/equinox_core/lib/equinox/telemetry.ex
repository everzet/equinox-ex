defmodule Equinox.Telemetry do
  @moduledoc """
  Equinox telemetry spans.
  """

  def span_codec_encode(codec, domain_event, context, fun) do
    meta = %{codec: codec, domain_event: domain_event, context: context}

    :telemetry.span([:equinox, :codec, :encode], meta, fn ->
      event_data = fun.()
      {event_data, Map.put(meta, :event_data, event_data)}
    end)
  end

  def span_codec_decode(codec, timeline_event, fun) do
    meta = %{codec: codec, timeline_event: timeline_event}

    :telemetry.span([:equinox, :codec, :decode], meta, fn ->
      domain_event = fun.()
      {domain_event, Map.put(meta, :domain_event, domain_event)}
    end)
  end

  def span_fold(fold, state, fun) do
    meta = %{fold: fold, original_state: state}

    :telemetry.span([:equinox, :fold], meta, fn ->
      folded_state = fun.()
      {folded_state, Map.put(meta, :folded_state, folded_state)}
    end)
  end

  def span_decider_load(decider, attempt, fun) do
    meta = %{original: decider, attempt: attempt, max_attempts: decider.max_load_attempts}

    :telemetry.span([:equinox, :decider, :load], meta, fn ->
      loaded = fun.()
      {loaded, Map.put(meta, :loaded, loaded)}
    end)
  end

  def span_decider_sync(decider, events, attempt, fun) do
    meta = %{
      original: decider,
      events: events,
      attempt: attempt,
      max_attempts: decider.max_sync_attempts
    }

    :telemetry.span([:equinox, :decider, :sync], meta, fn ->
      synced = fun.()
      {synced, Map.put(meta, :synced, synced)}
    end)
  end

  def span_decider_query(decider, query, fun) do
    meta = %{decider: decider, query_fun: query}
    :telemetry.span([:equinox, :decider, :query], meta, fn -> {fun.(), meta} end)
  end

  def span_decider_transact(decider, decision, context, fun) do
    meta = %{original_decider: decider, decision_fun: decision, context: context}

    :telemetry.span([:equinox, :decider, :transact], meta, fn ->
      case fun.() do
        {:ok, decider} ->
          {{:ok, decider}, Map.merge(meta, %{ok?: true, synced_decider: decider})}

        {:error, error} ->
          {{:error, error}, Map.merge(meta, %{ok?: false, error: error})}
      end
    end)
  end

  def span_decider_decision(decider, attempt, decision, context, fun) do
    meta = %{
      decider: decider,
      decision_fun: decision,
      context: context,
      attempt: attempt,
      max_attempts: decider.max_resync_attempts
    }

    :telemetry.span([:equinox, :decider, :transact, :decision], meta, fn ->
      case fun.() do
        {:ok, events} ->
          {{:ok, events}, Map.merge(meta, %{ok?: true, events: events})}

        {:error, error} ->
          {{:error, error}, Map.merge(meta, %{ok?: false, error: error})}
      end
    end)
  end

  def span_decider_resync(decider, attempt, fun) do
    meta = %{
      original_decider: decider,
      attempt: attempt,
      max_attempts: decider.max_resync_attempts
    }

    :telemetry.span([:equinox, :decider, :transact, :resync], meta, fn ->
      resynced_decider = fun.()
      {resynced_decider, Map.put(meta, :resynced_decider, resynced_decider)}
    end)
  end

  def decider_server_init(server) do
    :telemetry.execute(
      [:equinox, :decider, :server, :init],
      %{system_time: System.system_time()},
      %{settings: server.settings, decider: server.decider}
    )
  end

  def decider_server_stop(server, reason) do
    :telemetry.execute(
      [:equinox, :decider, :server, :stop],
      %{system_time: System.system_time()},
      %{settings: server.settings, decider: server.decider, reason: reason}
    )
  end

  def span_decider_server_load(server, fun) do
    meta = %{
      settings: server.settings,
      preloaded?: Equinox.Decider.Stateless.loaded?(server.decider),
      original_decider: server.decider
    }

    :telemetry.span([:equinox, :decider, :server, :load], meta, fn ->
      loaded_decider = fun.()
      {loaded_decider, Map.put(meta, :loaded_decider, loaded_decider)}
    end)
  end

  def span_decider_server_query(server, query, fun) do
    meta = %{settings: server.settings, decider: server.decider, query_fun: query}
    :telemetry.span([:equinox, :decider, :query], meta, fn -> {fun.(), meta} end)
  end

  def span_decider_server_transact(server, decision, context, fun) do
    meta = %{
      settings: server.settings,
      original_decider: server.decider,
      decision_fun: decision,
      context: context
    }

    :telemetry.span([:equinox, :decider, :server, :transact], meta, fn ->
      case fun.() do
        {:ok, decider} ->
          {{:ok, decider}, Map.merge(meta, %{ok?: true, synced_decider: decider})}

        {:error, error} ->
          {{:error, error}, Map.merge(meta, %{ok?: false, error: error})}
      end
    end)
  end
end
