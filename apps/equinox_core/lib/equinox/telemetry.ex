defmodule Equinox.Telemetry do
  @moduledoc """
  Equinox telemetry spans.
  """

  def span_codec_encode(codec, domain_event, ctx, encode_fun) do
    meta = %{codec: codec, domain_event: domain_event, ctx: ctx}

    :telemetry.span([:equinox, :codec, :encode], meta, fn ->
      event_data = encode_fun.()
      {event_data, Map.put(meta, :event_data, event_data)}
    end)
  end

  def span_codec_decode(codec, timeline_event, decode_fun) do
    meta = %{codec: codec, timeline_event: timeline_event}

    :telemetry.span([:equinox, :codec, :decode], meta, fn ->
      domain_event = decode_fun.()
      {domain_event, Map.put(meta, :domain_event, domain_event)}
    end)
  end

  def span_fold(fold, state, fold_fun) do
    meta = %{fold: fold, state: state}

    :telemetry.span([:equinox, :fold], meta, fn ->
      new_state = fold_fun.()
      {new_state, Map.put(meta, :new_state, new_state)}
    end)
  end

  alias Equinox.Decider.Stateless

  def span_decider_load(%Stateless{} = decider, attempt, load_fun) do
    meta =
      decider
      |> decider_metadata()
      |> Map.merge(%{attempt: attempt, max_attempts: decider.max_load_attempts})

    :telemetry.span([:equinox, :decider, :load], meta, fn -> {load_fun.(), meta} end)
  end

  def span_decider_sync(%Stateless{} = decider, events, attempt, sync_fun) do
    meta =
      decider
      |> decider_metadata()
      |> Map.merge(%{events: events, attempt: attempt, max_attempts: decider.max_sync_attempts})

    :telemetry.span([:equinox, :decider, :sync], meta, fn -> {sync_fun.(), meta} end)
  end

  def span_decider_query(%Stateless{} = decider, query_fun, execute_fun) do
    meta = decider |> decider_metadata() |> Map.put(:query_fun, query_fun)
    :telemetry.span([:equinox, :decider, :query], meta, fn -> {execute_fun.(), meta} end)
  end

  def span_decider_transact(%Stateless{} = decider, decision_fun, execute_fun) do
    meta = decider |> decider_metadata() |> Map.put(:decision_fun, decision_fun)

    :telemetry.span([:equinox, :decider, :transact], meta, fn ->
      case execute_fun.() do
        {:ok, decider} -> {{:ok, decider}, Map.put(meta, :ok?, true)}
        {:error, error} -> {{:error, error}, Map.merge(meta, %{ok?: false, error: error})}
      end
    end)
  end

  def span_decider_decision(%Stateless{} = decider, decision_fun, execute_fun) do
    meta = decider |> decider_metadata() |> Map.put(:decision_fun, decision_fun)

    :telemetry.span([:equinox, :decider, :transact, :decision_fun], meta, fn ->
      case execute_fun.() do
        {:ok, events} ->
          {{:ok, events}, Map.merge(meta, %{ok?: true, events: events})}

        {:error, error} ->
          {{:error, error}, Map.merge(meta, %{ok?: false, error: error})}
      end
    end)
  end

  def span_decider_resync(%Stateless{} = decider, attempt, resync_fun) do
    meta =
      decider
      |> decider_metadata()
      |> Map.merge(%{attempt: attempt, max_attempts: decider.max_resync_attempts})

    :telemetry.span([:equinox, :decider, :transact, :resync], meta, fn ->
      {resync_fun.(), meta}
    end)
  end

  defp decider_metadata(%Stateless{} = decider) do
    %{
      stream_name: decider.stream_name,
      state: decider.state,
      store: decider.store,
      codec: decider.codec,
      fold: decider.fold
    }
  end
end
