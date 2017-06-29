defmodule Postnord.IdGen do
  import Postnord.BinaryConvert

  @moduledoc """
  Efficiently generates unique ID's for various Postnord components.
  """

  @bytes 16
  @range 256 |> :math.pow(16) |> round()

  @doc """
  Returns a unique 128-bit message ID, as a 16-byte binary.

  ## Examples

      iex> Postnord.IdGen.message_id() |> byte_size()
      16

      iex> Postnord.IdGen.message_id() |> is_binary()
      true

      iex> id = Postnord.IdGen.message_id()
      iex> <<id_test::binary-size(16)>> = id
      iex> id_test == id
      true

      iex> Postnord.IdGen.message_id() == Postnord.IdGen.message_id()
      false
  """
  def node_id do
    RandomBytes.uuid()
  end

  @doc """
  Returns a unique 128-bit message ID, as a 16-byte binary.

  ## Examples

      iex> Postnord.IdGen.message_id() |> byte_size()
      16

      iex> Postnord.IdGen.message_id() |> is_binary()
      true

      iex> id = Postnord.IdGen.message_id()
      iex> <<id_test::binary-size(16)>> = id
      iex> id_test == id
      true

      iex> Postnord.IdGen.message_id() == Postnord.IdGen.message_id()
      false
  """
  def message_id do
    int = :rand.uniform(@range) - 1
    integer_to_binary(int, @bytes)
  end
end
