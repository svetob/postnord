defmodule Postnord.IndexLog.Entry do
  require Logger
  import Postnord.BinaryConvert
  import ExUnit.Assertions

  @moduledoc """
  Index log entry definition.

  An entry contains the ID, size, and disk location of a single message.
  """
  # TODO: Improve storage format

  defstruct id: <<>>,
            offset: 0,
            len: 0,
            timestamp: 0

  @id_size 16
  @entry_size 40
  def entry_size, do: @entry_size

  def as_bytes(entry) do
    assert byte_size(entry.id) == @id_size

    entry.id <>
      integer_to_binary(entry.offset, 8) <>
      integer_to_binary(entry.len, 8) <> integer_to_binary(entry.timestamp, 8)
  end

  def from_bytes(bytes) do
    assert byte_size(bytes) == @entry_size

    %Postnord.IndexLog.Entry{
      id: binary_part(bytes, 0, 16),
      offset: binary_to_integer(binary_part(bytes, 16, 8)),
      len: binary_to_integer(binary_part(bytes, 24, 8)),
      timestamp: binary_to_integer(binary_part(bytes, 32, 8))
    }
  end
end
