defmodule Equinox.Telemetry do
  @moduledoc """
  Equinox telemetry spans.
  """

  def span_codec_encode(codec, domain_event, ctx, fun) do
    meta = %{codec: codec, domain_event: domain_event, ctx: ctx}

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
    meta = %{fold: fold, old_state: state}

    :telemetry.span([:equinox, :fold], meta, fn ->
      new_state = fun.()
      {new_state, Map.put(meta, :new_state, new_state)}
    end)
  end

  alias Equinox.Decider.Stateless

  def span_decider_load(%Stateless{} = decider, attempt, fun) do
    meta =
      %{decider: decider, attempt: attempt, max_attempts: decider.max_load_attempts}

    :telemetry.span([:equinox, :decider, :load], meta, fn -> {fun.(), meta} end)
  end

  def span_decider_sync(%Stateless{} = decider, events, attempt, fun) do
    meta =
      %{
        decider: decider,
        events: events,
        attempt: attempt,
        max_attempts: decider.max_sync_attempts
      }

    :telemetry.span([:equinox, :decider, :sync], meta, fn -> {fun.(), meta} end)
  end

  def span_decider_query(%Stateless{} = decider, query_fun, fun) do
    meta = %{decider: decider, query_fun: query_fun}
    :telemetry.span([:equinox, :decider, :query], meta, fn -> {fun.(), meta} end)
  end

  def span_decider_transact(%Stateless{} = decider, decision, fun) do
    meta = %{decider: decider, decision_fun: decision}

    :telemetry.span([:equinox, :decider, :transact], meta, fn ->
      case fun.() do
        {:ok, decider} -> {{:ok, decider}, Map.put(meta, :ok?, true)}
        {:error, error} -> {{:error, error}, Map.merge(meta, %{ok?: false, error: error})}
      end
    end)
  end

  def span_decider_decision(%Stateless{} = decider, attempt, decision, fun) do
    meta = %{
      decider: decider,
      decision_fun: decision,
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

  def span_decider_resync(%Stateless{} = decider, attempt, fun) do
    meta = %{decider: decider, attempt: attempt, max_attempts: decider.max_resync_attempts}
    :telemetry.span([:equinox, :decider, :transact, :resync], meta, fn -> {fun.(), meta} end)
  end
end
