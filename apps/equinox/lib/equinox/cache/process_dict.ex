defmodule Equinox.Cache.ProcessDict do
  defstruct []

  def new(_ \\ []), do: %__MODULE__{}

  defimpl Equinox.Cache do
    @impl Equinox.Cache
    def get(_cache, stream_name, max_age) do
      current_time = System.monotonic_time(:millisecond)

      case Process.get(:decider_state_cache) do
        {^stream_name, cached_state, insert_time}
        when max_age == :infinity or max_age > current_time - insert_time ->
          cached_state

        _ ->
          nil
      end
    end

    @impl Equinox.Cache
    def put(_cache, stream_name, state) do
      current_time = System.monotonic_time(:millisecond)
      Process.put(:decider_state_cache, {stream_name, state, current_time})
    end
  end
end
