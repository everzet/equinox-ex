defmodule Equinox.MessageDb.Store do
  alias Equinox.MessageDb.{Reader, Writer}
  alias Equinox.Store.{State, EventsToSync}

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

        def load(stream, _policy, opts) do
          do_load(stream, nil, opts)
        end

        def sync(stream, state, to_sync, opts) do
          opts = merge_options!(opts)
          conn = opts[:write_conn]
          codec = opts[:codec]
          fold = opts[:fold]

          case MessageDb.Store.sync(conn, stream, state, to_sync, codec, fold) do
            {:error, %Writer.StreamVersionConflict{} = conflict} ->
              {:conflict, fn -> do_load(stream, state, opts) end}

            anything_else ->
              anything_else
          end
        end

        defp do_load(stream, state, opts) do
          opts = merge_options!(opts)
          conn = opts[:fetch_conn]
          codec = opts[:codec]
          fold = opts[:fold]
          batch_size = opts[:batch_size]

          MessageDb.Store.load_unoptimized(conn, stream, nil, codec, fold, batch_size)
        end

        defp merge_options!(opts) do
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

        def load(stream, _policy, opts) do
          do_load(stream, nil, opts)
        end

        def sync(stream, state, to_sync, opts) do
          opts = merge_options!(opts)
          conn = opts[:write_conn]
          codec = opts[:codec]
          fold = opts[:fold]

          case MessageDb.Store.sync(conn, stream, state, to_sync, codec, fold) do
            {:error, %Writer.StreamVersionConflict{} = conflict} ->
              {:conflict, fn -> do_load(stream, state, opts) end}

            anything_else ->
              anything_else
          end
        end

        defp do_load(stream, state, opts) do
          opts = merge_options!(opts)
          conn = opts[:fetch_conn]
          codec = opts[:codec]
          fold = opts[:fold]

          MessageDb.Store.load_latest_known_event(conn, stream, state, codec, fold)
        end

        defp merge_options!(opts) do
          @opts
          |> Keyword.merge(opts)
          |> Options.validate!()
        end

        defoverridable Equinox.Store
      end
    end
  end

  def sync(conn, stream_name, state, to_sync, codec, fold) do
    messages = EventsToSync.to_messages(to_sync, codec)

    case Writer.write_messages(conn, stream_name, messages, state.version) do
      {:ok, new_version} ->
        new_state =
          to_sync.events
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
        case codec.decode(timeline_event) do
          nil ->
            {:cont, {:ok, state}}

          evt ->
            new_state =
              [evt]
              |> fold.fold(state.value)
              |> State.new(timeline_event.position)

            {:cont, {:ok, new_state}}
        end

      {:error, error}, {:ok, _partial_state} ->
        {:halt, {:error, error}}
    end)
  end

  def load_latest_known_event(conn, stream_name, state, codec, fold) do
    state = State.ensure_initialized(state, fold, -1)

    case Reader.get_last_stream_message(conn, stream_name) do
      {:ok, nil} ->
        {:ok, state}

      {:ok, timeline_event} ->
        case codec.decode(timeline_event) do
          nil ->
            {:ok, state}

          evt ->
            new_state =
              [evt]
              |> fold.fold(state.value)
              |> State.new(timeline_event.position)

            {:ok, new_state}
        end

      {:error, error} ->
        {:error, error}
    end
  end
end
