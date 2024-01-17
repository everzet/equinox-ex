defmodule Equinox.MessageDb.Store do
  alias Equinox.MessageDb.{Reader, Writer}
  alias Equinox.Store.{State, Outcome}

  defmodule Unoptimized do
    defmodule Options do
      @opts NimbleOptions.new!(
              fetch_conn: [
                type: :atom,
                required: true,
                doc: "Read connection module"
              ],
              write_conn: [
                type: :atom,
                required: true,
                doc: "Write connection module"
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
              batch_size: [
                type: :pos_integer,
                default: 500,
                doc: "Number of events to read at once"
              ]
            )

      @type t :: [o()]
      @type o :: unquote(NimbleOptions.option_typespec(@opts))

      def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
      def docs, do: NimbleOptions.docs(@opts)
    end

    defmacro __using__(opts) do
      quote do
        @behaviour Equinox.Store
        @opts unquote(opts)

        alias Equinox.MessageDb

        def load(stream, state, _policy, opts) do
          o = parse_options!(opts)

          MessageDb.Store.load_unoptimized(
            o[:fetch_conn],
            stream,
            state,
            o[:codec],
            o[:fold],
            o[:batch_size]
          )
        end

        def sync(stream, state, outcome, opts) do
          o = parse_options!(opts)
          MessageDb.Store.sync(o[:write_conn], stream, state, outcome, o[:codec], o[:fold])
        end

        def parse_options!(opts) do
          @opts
          |> Keyword.merge(opts)
          |> Options.validate!()
        end

        defoverridable Equinox.Store
      end
    end
  end

  defmodule LatestKnownEvent do
    defmodule Options do
      @opts NimbleOptions.new!(
              fetch_conn: [
                type: :atom,
                required: true,
                doc: "Read connection module"
              ],
              write_conn: [
                type: :atom,
                required: true,
                doc: "Write connection module"
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
              ]
            )

      @type t :: [o()]
      @type o :: unquote(NimbleOptions.option_typespec(@opts))

      def validate!(opts), do: NimbleOptions.validate!(opts, @opts)
      def docs, do: NimbleOptions.docs(@opts)
    end

    defmacro __using__(opts) do
      quote do
        @behaviour Equinox.Store
        @opts unquote(opts)

        alias Equinox.MessageDb

        def load(stream, state, _policy, opts) do
          o = parse_options!(opts)

          MessageDb.Store.load_latest_known_event(
            o[:fetch_conn],
            stream,
            state,
            o[:codec],
            o[:fold]
          )
        end

        def sync(stream, state, outcome, opts) do
          o = parse_options!(opts)
          MessageDb.Store.sync(o[:write_conn], stream, state, outcome, o[:codec], o[:fold])
        end

        def parse_options!(opts) do
          @opts
          |> Keyword.merge(opts)
          |> Options.validate!()
        end

        defoverridable Equinox.Store
      end
    end
  end

  def sync(conn, stream_name, state, outcome, codec, fold) do
    messages = Outcome.produce_messages(outcome, codec)

    case Writer.write_messages(conn, stream_name, messages, state.version) do
      {:ok, new_version} ->
        new_state =
          outcome.events
          |> fold.fold(state.value)
          |> State.new(new_version)

        {:ok, new_state}

      {:error, error} ->
        {:error, error}
    end
  end

  def load_unoptimized(conn, stream_name, state, codec, fold, batch_size) do
    state = State.ensure_initialized(state, fold, -1)

    conn
    |> Reader.stream_messages(stream_name, state.version + 1, batch_size)
    |> Enum.reduce_while({:ok, state}, fn
      {:ok, timeline_event}, {:ok, state} ->
        new_state =
          timeline_event
          |> codec.decode()
          |> then(&fold.fold([&1], state.value))
          |> State.new(timeline_event.position)

        {:cont, {:ok, new_state}}

      {:error, error}, {:ok, partial_state} ->
        {:halt, {:error, error, partial_state}}
    end)
  end

  def load_latest_known_event(conn, stream_name, state, codec, fold) do
    state = State.ensure_initialized(state, fold, -1)

    case Reader.get_last_stream_message(conn, stream_name) do
      {:ok, nil} ->
        {:ok, state}

      {:ok, timeline_event} ->
        new_state =
          timeline_event
          |> codec.decode()
          |> then(&fold.fold([&1], state.value))
          |> State.new(timeline_event.position)

        {:ok, new_state}

      {:error, error} ->
        {:error, error, state}
    end
  end
end
