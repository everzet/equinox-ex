defmodule Equinox.UUID do
  @moduledoc """
  Generates and validates UUID's of version 4

  This is mostly a copy-paste from [Ecto](https://github.com/elixir-ecto/ecto).
  Mad respect to original creators and contributors!

  ## License

  At the time of copying, the original Ecto code is licensed under Apache 2.0.
  This implementation removes some of Ecto-specific casting, but keeps the original
  UUID generation logic. It also adds very basic UUID validation functions via
  parse/1.

  Copyright (c) 2013 Plataformatec \
  Copyright (c) 2020 Dashbit

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  [https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0)

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
  """

  @typedoc """
  A hex-encoded UUID string.
  """
  @type t :: <<_::288>>

  @typedoc """
  A raw binary representation of a UUID.
  """
  @type raw :: <<_::128>>

  @doc """
  Parses given string as UUID by validating and downcasting it.
  """
  @spec parse(String.t()) :: {:ok, String.t()} | {:error, Exception.t()}
  def parse(str) when is_bitstring(str) do
    if String.match?(
         str,
         ~r/^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$/
       ) do
      {:ok, String.downcase(str)}
    else
      {:error, %ArgumentError{message: "UUID: Expected a valid UUID, but got: #{inspect(str)}"}}
    end
  end

  @doc """
  Generates a random, version 4 UUID.
  """
  @spec generate() :: t
  def generate(), do: encode(bingenerate())

  @doc """
  Generates a random, version 4 UUID in the binary format.
  """
  @spec bingenerate() :: raw
  def bingenerate() do
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)
    <<u0::48, 4::4, u1::12, 2::2, u2::62>>
  end

  @spec encode(raw) :: t
  defp encode(
         <<a1::4, a2::4, a3::4, a4::4, a5::4, a6::4, a7::4, a8::4, b1::4, b2::4, b3::4, b4::4,
           c1::4, c2::4, c3::4, c4::4, d1::4, d2::4, d3::4, d4::4, e1::4, e2::4, e3::4, e4::4,
           e5::4, e6::4, e7::4, e8::4, e9::4, e10::4, e11::4, e12::4>>
       ) do
    <<e(a1), e(a2), e(a3), e(a4), e(a5), e(a6), e(a7), e(a8), ?-, e(b1), e(b2), e(b3), e(b4), ?-,
      e(c1), e(c2), e(c3), e(c4), ?-, e(d1), e(d2), e(d3), e(d4), ?-, e(e1), e(e2), e(e3), e(e4),
      e(e5), e(e6), e(e7), e(e8), e(e9), e(e10), e(e11), e(e12)>>
  end

  @compile {:inline, e: 1}

  defp e(0), do: ?0
  defp e(1), do: ?1
  defp e(2), do: ?2
  defp e(3), do: ?3
  defp e(4), do: ?4
  defp e(5), do: ?5
  defp e(6), do: ?6
  defp e(7), do: ?7
  defp e(8), do: ?8
  defp e(9), do: ?9
  defp e(10), do: ?a
  defp e(11), do: ?b
  defp e(12), do: ?c
  defp e(13), do: ?d
  defp e(14), do: ?e
  defp e(15), do: ?f
end
