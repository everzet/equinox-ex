defmodule Equinox.Stream do
  defmodule Errors do
    defmodule ElementError do
      @enforce_keys [:message]
      defexception [:message]
      @type t :: %__MODULE__{message: String.t()}
    end

    @type t :: ElementError.t()
  end

  defmodule StreamId do
    @separator "_"

    @spec generate(nonempty_list(String.t())) :: String.t()
    def generate(elements) when is_list(elements) and length(elements) > 0 do
      if Enum.any?(elements, &String.contains?(&1, @separator)) do
        raise Errors.ElementError,
          message:
            "StreamId: Expected elements to not contain #{@separator}, but got: #{inspect(elements)}"
      end

      Enum.join(elements, @separator)
    end

    @spec parse!(String.t()) :: nonempty_list(String.t())
    def parse!(val) do
      case parse(val) do
        {:ok, stream_id} -> stream_id
        {:error, error} -> raise error
      end
    end

    @spec parse(String.t()) :: {:ok, nonempty_list(String.t())} | {:error, Errors.t()}
    def parse(string) do
      case string do
        "" ->
          {:error,
           %Errors.ElementError{message: "StreamId: Expected non-empty string, but got one"}}

        val when not is_bitstring(val) ->
          {:error,
           %Errors.ElementError{message: "StreamId: Expected a string, but got #{inspect(val)}"}}

        _ ->
          {:ok, String.split(string, @separator)}
      end
    end
  end

  defmodule StreamName do
    @separator "-"

    @spec generate(String.t(), String.t()) :: String.t()
    def generate(category, stream_id) when is_bitstring(category) and is_bitstring(stream_id) do
      if String.contains?(category, @separator) do
        raise Errors.ElementError,
          message:
            "StreamName: Expected category to not contain #{@separator}, but got: #{inspect(category)}"
      end

      Enum.join([category, stream_id], @separator)
    end

    @spec match(String.t(), String.t()) :: {:ok, nonempty_list(String.t())} | {:error, Errors.t()}
    def match(expected_category, string) when is_bitstring(string) do
      with {:ok, {category, stream_id}} when category == expected_category <- parse(string) do
        {:ok, stream_id}
      else
        {:ok, _not_matching_stream_name} ->
          {:error,
           %Errors.ElementError{
             message:
               "StreamName: Expected a stream under category of #{expected_category}, but got '#{string}'"
           }}
      end
    end

    @spec parse!(String.t()) :: {category :: String.t(), stream_id :: nonempty_list(String.t())}
    def parse!(val) do
      case parse(val) do
        {:ok, stream_name} -> stream_name
        {:error, error} -> raise error
      end
    end

    @spec parse(String.t()) ::
            {:ok, {category :: String.t(), stream_id :: nonempty_list(String.t())}}
            | {:error, Errors.t()}

    def parse(val) when not is_bitstring(val) do
      {:error,
       %Errors.ElementError{message: "StreamName: Expected a string, but got: #{inspect(val)}"}}
    end

    def parse(string) do
      with [category, stream_id_str] <- String.split(string, @separator, parts: 2),
           {:ok, stream_id} <- StreamId.parse(stream_id_str) do
        {:ok, {category, stream_id}}
      else
        list when is_list(list) ->
          {:error,
           %Errors.ElementError{
             message:
               "StreamName: Expected a string with 2 elements separated by #{@separator}, but got: #{inspect(string)}"
           }}
      end
    end
  end
end
