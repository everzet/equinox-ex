defmodule Equinox.Codec.StreamId do
  defmodule Fragment do
    defmodule Error do
      defexception [:message]
      @type t :: %__MODULE__{message: String.t()}
    end

    @separator "_"
    def separator, do: @separator

    def validate!(raw) when is_bitstring(raw) do
      if String.trim(raw) == "" do
        raise Error, "StreamId: Fragments must not be empty"
      end

      if String.contains?(raw, @separator) do
        raise Error, "StreamId: Fragments must not contain '#{@separator}', but got '#{raw}'"
      end
    end
  end

  defmodule Fragments do
    defmodule Error do
      defexception [:message]
      @type t :: %__MODULE__{message: String.t()}
    end

    def compose(fragments) do
      Enum.each(fragments, &Fragment.validate!/1)
      Enum.join(fragments, Fragment.separator())
    end

    def split(string, count) do
      fragments = String.split(string, Fragment.separator())

      if length(fragments) == count do
        {:ok, fragments}
      else
        {:error, Error.exception("StreamId: Expected #{count} fragments, but got '#{string}'")}
      end
    end
  end

  @enforce_keys [:fragments, :combined]
  defstruct [:fragments, :combined]

  @type t :: %__MODULE__{
          fragments: nonempty_list(String.t()),
          combined: String.t()
        }

  @spec new(String.t() | nonempty_list(String.t())) :: t()
  def new(fragments) do
    fragments = List.wrap(fragments)
    combined = Fragments.compose(fragments)
    %__MODULE__{fragments: fragments, combined: combined}
  end

  @spec encode(t()) :: String.t()
  def encode(%__MODULE__{combined: combined}), do: combined

  @spec decode(String.t(), pos_integer()) :: {:ok, t()} | {:error, Fragments.Error.t()}
  def decode(combined, count) do
    with {:ok, fragments} <- Fragments.split(combined, count) do
      {:ok, %__MODULE__{fragments: fragments, combined: combined}}
    end
  end

  @spec decode!(String.t(), pos_integer()) :: t()
  def decode!(combined, count) do
    case decode(combined, count) do
      {:ok, stream_id} -> stream_id
      {:error, error} -> raise error
    end
  end

  defimpl String.Chars do
    def to_string(stream_id), do: stream_id.combined
  end
end
