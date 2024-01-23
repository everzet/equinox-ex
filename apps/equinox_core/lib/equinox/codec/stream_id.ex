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

  @type t :: String.t()

  @spec encode(String.t()) :: t()
  def encode(id) when is_bitstring(id), do: Fragments.compose([id])

  @spec encode({String.t(), String.t()}) :: t()
  @spec encode({String.t(), String.t(), String.t()}) :: t()
  @spec encode({String.t(), String.t(), String.t(), String.t()}) :: t()
  def encode(tuple) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> Fragments.compose()

  @spec decode(t(), 1) ::
          {:ok, String.t()}
          | {:error, Fragments.Error.t()}
  @spec decode(t(), 2) ::
          {:ok, {String.t(), String.t()}}
          | {:error, Fragments.Error.t()}
  @spec decode(t(), 3) ::
          {:ok, {String.t(), String.t(), String.t()}}
          | {:error, Fragments.Error.t()}
  @spec decode(t(), 4) ::
          {:ok, {String.t(), String.t(), String.t(), String.t()}}
          | {:error, Fragments.Error.t()}
  def decode(string, count) do
    with {:ok, fragments} <- Fragments.split(string, count) do
      case fragments do
        [id] -> {:ok, id}
        list -> {:ok, List.to_tuple(list)}
      end
    end
  end

  @spec decode!(String.t(), 1) :: String.t()
  @spec decode!(String.t(), 2) :: {String.t(), String.t()}
  @spec decode!(String.t(), 3) :: {String.t(), String.t(), String.t()}
  @spec decode!(String.t(), 4) :: {String.t(), String.t(), String.t(), String.t()}
  def decode!(string, count) do
    case decode(string, count) do
      {:ok, res} -> res
      {:error, error} -> raise error
    end
  end
end
