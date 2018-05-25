defmodule Postnord.Id do
  import Postnord.BinaryConvert

  @moduledoc """
  Efficiently generates unique ID's for various Postnord components.
  """

  @bytes 16
  @range 256 |> :math.pow(16) |> round()

  @doc """
  Returns a unique 128-bit message ID, as a 16-byte binary.

  ## Examples

      iex> Postnord.Id.message_id() |> byte_size()
      16

      iex> Postnord.Id.message_id() |> is_binary()
      true

      iex> id = Postnord.Id.message_id()
      iex> <<id_test::binary-size(16)>> = id
      iex> id_test == id
      true

      iex> Postnord.Id.message_id() == Postnord.Id.message_id()
      false
  """
  def node_id do
    RandomBytes.uuid()
  end

  def partition_id do
    RandomBytes.uuid()
  end

  @doc """
  Returns a unique 128-bit message ID, as a 16-byte binary.

  ## Examples

      iex> Postnord.Id.message_id() |> byte_size()
      16

      iex> Postnord.Id.message_id() |> is_binary()
      true

      iex> id = Postnord.Id.message_id()
      iex> <<id_test::binary-size(16)>> = id
      iex> id_test == id
      true

      iex> Postnord.Id.message_id() == Postnord.Id.message_id()
      false
  """
  def message_id do
    int = :rand.uniform(@range) - 1
    integer_to_binary(int, @bytes)
  end

  # TODO doctest
  def message_id_encode(id) do
    Base.url_encode64(id, padding: false)
  end

  # TODO doctest
  def message_id_decode(id) do
    Base.url_decode64(id, padding: false)
  end
end
