defmodule Equinox.Codec.StreamName do
  defmodule Category do
    defmodule Error do
      defexception [:message]
      @type t :: %__MODULE__{message: String.t()}
    end

    @type t :: String.t()

    @separator "-"
    def separator, do: @separator

    def validate!(raw) when is_bitstring(raw) do
      if String.trim(raw) == "" do
        raise Error, "Category: must not be empty"
      end

      if String.contains?(raw, @separator) do
        raise Error, "Category: must not contain '#{@separator}', but got '#{raw}'"
      end
    end
  end

  defmodule Fragments do
    defmodule Error do
      defexception [:message]
      @type t :: %__MODULE__{message: String.t()}
    end

    def compose(category, stream_id) do
      Category.validate!(category)
      "#{category}#{Category.separator()}#{stream_id}"
    end

    def split(stream_name) do
      fragments = String.split(stream_name, Category.separator(), parts: 2)

      if length(fragments) == 2 do
        {:ok, fragments}
      else
        {:error,
         Error.exception(
           "StreamName: Expected to contain a '#{Category.separator()}', but got '#{fragments}'"
         )}
      end
    end
  end

  defmodule WrongCategory do
    defexception [:message]
    @type t :: %__MODULE__{message: String.t()}

    def exception(category) do
      %__MODULE__{message: "StreamName: Received unexpected category '#{category}'"}
    end
  end

  alias Equinox.Codec.StreamId

  @enforce_keys [:category, :id, :full]
  defstruct [:category, :id, :full]
  @type t :: %__MODULE__{category: Category.t(), id: StreamId.t(), full: String.t()}

  @spec new(Category.t(), StreamId.t()) :: t()
  def new(category, id) when is_bitstring(id) do
    %__MODULE__{category: category, id: id, full: Fragments.compose(category, id)}
  end

  @spec decode(String.t()) :: {:ok, t()} | {:error, Fragments.Error.t()}
  def decode(stream_name) when is_bitstring(stream_name) do
    with {:ok, [category, id]} <- Fragments.split(stream_name) do
      {:ok, %__MODULE__{category: category, id: id, full: stream_name}}
    end
  end

  @spec decode!(String.t()) :: t()
  def decode!(stream_name) do
    case decode(stream_name) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  defimpl String.Chars do
    def to_string(stream_name), do: stream_name.full
  end
end
