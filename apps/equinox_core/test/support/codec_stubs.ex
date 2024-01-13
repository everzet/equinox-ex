defmodule Equinox.CodecStubs do
  defmodule TestStruct do
    @enforce_keys [:val1]
    defstruct [:val1, :val2]
  end

  defmodule UpcastableTestStruct do
    defstruct [:val1, :val2]

    defimpl Equinox.Codec.EventStructs.Upcast do
      def upcast(struct), do: %TestStruct{val1: struct.val1, val2: struct.val2 + 1}
    end
  end

  defmodule DowncastableTestStruct do
    defstruct [:val1, :val2]

    defimpl Equinox.Codec.EventStructs.Downcast do
      def downcast(struct), do: %TestStruct{val1: struct.val1, val2: struct.val2 - 1}
    end
  end
end
