defmodule Postnord.IdGen do
  import Postnord.BinaryConvert

  @moduledoc """
  Efficiently generates unique 128-bit message ID's
  """

  @bytes 16
  @range 256 |> :math.pow(16) |> round()

  @doc """
  Returns a 128-bit ID, as a 16-byte binary.

  ## Examples

      iex> Postnord.IdGen.id() |> byte_size()
      16

      iex> Postnord.IdGen.id() |> is_binary()
      true

      iex> id = Postnord.IdGen.id()
      iex> <<id_test::binary-size(16)>> = id
      iex> id_test == id
      true

      iex> Postnord.IdGen.id() == Postnord.IdGen.id()
      false
  """
  def id do
    int = :rand.uniform(@range) - 1
    integer_to_binary(int, @bytes)
  end
end
