defmodule Equinox.Stream do
  defmodule ElementError do
    defexception [:message]
    @type t :: %__MODULE__{message: String.t()}
  end

  defmodule StreamId do
    @enforce_keys [:elements]
    defstruct [:elements]

    @type t :: %__MODULE__{elements: nonempty_list(String.t())}

    def separator, do: "_"

    @spec new(nonempty_list(String.t())) :: t()
    def new(elements) when is_list(elements) and length(elements) > 0 do
      if Enum.any?(elements, &String.contains?(&1, separator())) do
        raise %ElementError{
          message:
            "StreamId: Expected elements to not contain #{separator()}, but got: '#{elements}'"
        }
      end

      %StreamId{elements: elements}
    end

    @spec parse(String.t()) :: {:ok, t()} | {:error, ElementError.t()}
    def parse(string) when is_bitstring(string) do
      {:ok, %StreamId{elements: String.split(string, separator())}}
    end

    defimpl String.Chars do
      def to_string(%StreamId{elements: e}), do: Enum.join(e, StreamId.separator())
    end
  end

  defmodule Category do
    @enforce_keys [:name]
    defstruct [:name]

    @type t :: %__MODULE__{name: String.t()}

    def separator, do: "-"

    @spec new(String.t()) :: t()
    def new(name) do
      if String.contains?(name, separator()) do
        raise %ElementError{
          message: "Category: Expected name to not contain #{separator()}, but got: '#{name}'"
        }
      end

      %Category{name: name}
    end

    @spec parse(String.t()) :: {:ok, t()} | {:error, ElementError.t()}
    def parse(string) when is_bitstring(string) do
      {:ok, %Category{name: string}}
    end

    defimpl String.Chars do
      def to_string(%Category{name: n}), do: n
    end
  end

  defmodule StreamName do
    @enforce_keys [:category, :stream_id]
    defstruct [:category, :stream_id]

    @type t :: %__MODULE__{category: Category.t(), stream_id: StreamId.t()}

    @spec new(Category.t(), StreamId.t()) :: t()
    def new(%Category{} = category, %StreamId{} = stream_id) do
      %StreamName{category: category, stream_id: stream_id}
    end

    @spec match(Category.t(), String.t()) :: {:ok, t()} | {:error, ElementError.t()}
    def match(%Category{name: expected_name}, string) when is_bitstring(string) do
      with {:ok, stream_name} when stream_name.category.name == expected_name <- parse(string) do
        {:ok, stream_name}
      else
        {:ok, _} ->
          {:error,
           %ElementError{
             message:
               "StreamName: Expected a stream under category of #{expected_name}, but got '#{string}'"
           }}
      end
    end

    @spec parse(String.t()) :: {:ok, t()} | {:error, ElementError.t()}
    def parse(string) when is_bitstring(string) do
      with [category_str, stream_id_str] <- String.split(string, Category.separator(), parts: 2),
           {:ok, category} <- Category.parse(category_str),
           {:ok, stream_id} <- StreamId.parse(stream_id_str) do
        {:ok, new(category, stream_id)}
      else
        list when is_list(list) ->
          {:error,
           %ElementError{
             message:
               "StreamName: Expected to contain 2 elements separated by #{Category.separator()}, but got: '#{string}'"
           }}
      end
    end

    defimpl String.Chars do
      def to_string(%StreamName{category: c, stream_id: s}), do: "#{c}#{Category.separator()}#{s}"
    end
  end
end
