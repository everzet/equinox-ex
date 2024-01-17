defmodule Equinox.Codec.StreamName do
  defmodule Category do
    defmodule Error do
      defexception [:message]
      @type t :: %__MODULE__{message: String.t()}
    end

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

  @spec encode(String.t(), String.t()) :: String.t()
  def encode(category_name, stream_id) when is_bitstring(stream_id) do
    Fragments.compose(category_name, stream_id)
  end

  @spec parse(String.t()) :: {:ok, String.t()} | {:error, Fragments.Error.t()}
  def parse(stream_name) do
    case Fragments.split(stream_name) do
      {:ok, [_category, _stream_id]} -> {:ok, stream_name}
      {:error, error} -> {:error, error}
    end
  end

  @spec parse!(String.t()) :: String.t()
  def parse!(stream_name) do
    case parse(stream_name) do
      {:ok, stream_name} -> stream_name
      {:error, error} -> raise error
    end
  end

  @spec decode(String.t()) :: {:ok, {String.t(), String.t()}} | {:error, Fragments.Error.t()}
  def decode(stream_name) do
    with {:ok, [category, stream_id]} <- Fragments.split(stream_name) do
      {:ok, {category, stream_id}}
    end
  end

  @spec decode!(String.t()) :: {String.t(), String.t()}
  def decode!(stream_name) do
    case decode(stream_name) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @spec match(String.t(), String.t()) ::
          {:ok, stream_id :: String.t()}
          | {:error, Fragments.Error.t()}
          | {:error, WrongCategory.t()}
  def match(match_category, stream_name) do
    with {:ok, {category_name, stream_id}} <- decode(stream_name) do
      if category_name == match_category do
        {:ok, stream_id}
      else
        {:error, WrongCategory.exception(category_name)}
      end
    end
  end

  @spec match!(String.t(), String.t()) :: stream_id :: String.t()
  def match!(match_category, stream_name) do
    case match(match_category, stream_name) do
      {:ok, stream_id} -> stream_id
      {:error, exception} -> raise exception
    end
  end
end
