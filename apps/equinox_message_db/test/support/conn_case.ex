defmodule Equinox.MessageDb.ConnCase do
  use ExUnit.CaseTemplate, async: true

  using do
    quote do
      import unquote(__MODULE__)
    end
  end

  setup tags do
    {:ok, conn: start_supervised!({Equinox.MessageDb.Connection, Enum.to_list(tags)})}
  end

  defmacro test_in_isolation(message, var, do: block) do
    quote do
      test unquote(message), context do
        Postgrex.transaction(context[:conn], fn t_conn ->
          unquote(var) = Map.put(context, :conn, t_conn)
          unquote(block)
          Postgrex.rollback(t_conn, :end_of_test)
        end)
      end
    end
  end
end
