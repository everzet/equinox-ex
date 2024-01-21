defmodule Equinox.Telemetry do
  @moduledoc """
  Equinox telemetry spans.
  """

  def span_decider_load(decider, load, fun) do
    meta = %{decider: decider, load_policy: load}
    :telemetry.span([:equinox, :decider, :load], meta, fn -> {fun.(), meta} end)
  end

  def span_decider_query(decider, query, load, fun) do
    meta = %{decider: decider, query: query, load_policy: load}
    :telemetry.span([:equinox, :decider, :query], meta, fn -> {fun.(), meta} end)
  end

  def span_decider_query_execute(decider, state, query, fun) do
    meta = %{decider: decider, state: state, query: query}

    :telemetry.span([:equinox, :decider, :query, :execute], meta, fn ->
      then(fun.(), &{&1, Map.put(meta, :result, &1)})
    end)
  end

  def span_decider_transact(decider, decision, load, fun) do
    meta = %{decider: decider, decision: decision, load_policy: load}

    :telemetry.span([:equinox, :decider, :transact], meta, fn ->
      case fun.() do
        {:error, err} -> {{:error, err}, Map.merge(meta, %{error: err})}
        res -> {res, Map.merge(meta, %{result: res})}
      end
    end)
  end

  def span_decider_transact_attempt(decider, state, decision, attempt, fun) do
    meta = %{decider: decider, state: state, decision: decision, attempt: attempt}

    :telemetry.span([:equinox, :decider, :transact, :attempt], meta, fn ->
      case fun.() do
        {:ok, res} -> {{:ok, res}, Map.merge(meta, %{result: res})}
        {:error, err} -> {{:error, err}, Map.merge(meta, %{error: err})}
        {:conflict, fun} -> {{:conflict, fun}, Map.merge(meta, %{error: :conflict})}
      end
    end)
  end

  def span_decider_transact_decision(decider, state, decision, fun) do
    meta = %{decider: decider, state: state, decision: decision}

    :telemetry.span([:equinox, :decider, :transact, :attempt, :decision], meta, fn ->
      case fun.() do
        {:ok, res, evt} -> {{:ok, res, evt}, Map.merge(meta, %{result: res, events: evt})}
        {:error, error} -> {{:error, error}, Map.merge(meta, %{error: error})}
      end
    end)
  end

  def span_decider_sync(decider, state, events, fun) do
    meta = %{decider: decider, state: state, events: events}

    :telemetry.span([:equinox, :decider, :sync], meta, fn ->
      case fun.() do
        {:ok, snc} -> {{:ok, snc}, Map.merge(meta, %{synced_state: snc})}
        {:error, err} -> {{:error, err}, Map.merge(meta, %{error: err})}
        {:conflict, fun} -> {{:conflict, fun}, Map.merge(meta, %{error: :conflict})}
      end
    end)
  end

  def async_server_init(server_state) do
    :telemetry.execute(
      [:equinox, :decider, :async, :init],
      %{
        system_time: System.system_time(),
        monotonic_time: server_state.init_time
      },
      server_state
    )
  end

  def async_server_shutdown(server_state, reason) do
    shutdown_time = System.monotonic_time()

    :telemetry.execute(
      [:equinox, :decider, :async, :shutdown],
      %{
        system_time: System.system_time(),
        monotonic_time: shutdown_time,
        duration: shutdown_time - server_state.init_time
      },
      Map.put(server_state, :reason, reason)
    )
  end
end
