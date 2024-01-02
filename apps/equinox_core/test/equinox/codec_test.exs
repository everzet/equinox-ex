defmodule Equinox.CodecTest do
  use ExUnit.Case, async: true

  alias Equinox.TestMocks.CodecMock
  alias Equinox.Codec.Errors
  alias Equinox.Codec

  import Mox

  setup :verify_on_exit!

  describe "encode_list!/3" do
    test "performs Codec.encode/2 on every given event" do
      expect(CodecMock, :encode, fn 1, %{c: 1} -> {:ok, :one} end)
      expect(CodecMock, :encode, fn 2, %{c: 1} -> {:ok, :two} end)
      assert Codec.encode_list!([1, 2], %{c: 1}, CodecMock) == [:one, :two]
    end

    test "raises exception if Codec returns {:error, exception}" do
      expect(CodecMock, :encode, fn _, _ -> {:error, %Errors.EncodeError{message: "bang"}} end)

      assert_raise Errors.EncodeError, ~r/bang/, fn ->
        Codec.encode_list!([1], %{c: 1}, CodecMock)
      end
    end

    test "raises exception if Codec returns {:error, term}" do
      expect(CodecMock, :encode, fn _, _ -> {:error, :bang} end)

      assert_raise Errors.EncodeError, ~r/:bang/, fn ->
        Codec.encode_list!([1], %{c: 1}, CodecMock)
      end
    end

    test "wraps all exceptions into EncodeError" do
      expect(CodecMock, :encode, fn _, _ -> raise RuntimeError end)

      assert_raise Errors.EncodeError, ~r/runtime error/, fn ->
        Codec.encode_list!([1], %{c: 1}, CodecMock)
      end
    end
  end

  describe "decode_stream!/2" do
    test "performs Codec.decode/1 on every given event with position" do
      expect(CodecMock, :decode, fn %{v: :one, position: 3} -> {:ok, 1} end)
      expect(CodecMock, :decode, fn %{v: :two, position: 4} -> {:ok, 2} end)

      result =
        Codec.decode_stream!(
          [%{v: :one, position: 3}, %{v: :two, position: 4}],
          CodecMock
        )

      assert Enum.to_list(result) == [{1, 3}, {2, 4}]
    end

    test "works with streams" do
      expect(CodecMock, :decode, fn %{v: :one, position: 3} -> {:ok, 1} end)
      expect(CodecMock, :decode, fn %{v: :two, position: 4} -> {:ok, 2} end)

      result =
        Codec.decode_stream!(
          Stream.map([%{v: :one, position: 3}, %{v: :two, position: 4}], & &1),
          CodecMock
        )

      assert Enum.to_list(result) == [{1, 3}, {2, 4}]
    end

    test "raises exception if Codec returns {:error, exception}" do
      expect(CodecMock, :decode, fn _ -> {:error, %Errors.DecodeError{message: "bang"}} end)

      assert_raise Errors.DecodeError, ~r/bang/, fn ->
        Stream.run(Codec.decode_stream!([%{v: :one, position: 0}], CodecMock))
      end
    end

    test "raises exception if Codec returns {:error, term}" do
      expect(CodecMock, :decode, fn _ -> {:error, :bang} end)

      assert_raise Errors.DecodeError, ~r/:bang/, fn ->
        Stream.run(Codec.decode_stream!([%{v: :one, position: 0}], CodecMock))
      end
    end

    test "wraps all exceptions into DecodeError" do
      expect(CodecMock, :decode, fn _ -> raise RuntimeError end)

      assert_raise Errors.DecodeError, ~r/runtime error/, fn ->
        Stream.run(Codec.decode_stream!([%{v: :one, position: 0}], CodecMock))
      end
    end
  end
end
