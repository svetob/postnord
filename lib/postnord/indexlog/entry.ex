defmodule Postnord.IndexLog.Entry do
  require Logger
  import Postnord.BinaryConvert

  @moduledoc """
  Index log entry definition.

  An entry contains the ID, size, and disk location of a single message.
  """
  # TODO: Improve storage format

  defstruct id: <<>>,
            offset: 0,
            len: 0

  @byte_size 32
  def byte_size, do: @byte_size

  def as_bytes(entry) do
    entry.id <> integer_to_binary(entry.offset, 8) <> integer_to_binary(entry.len, 8)
  end

  def from_bytes(bytes) do
    # TODO Assert bytes size
    %Postnord.IndexLog.Entry{id:     binary_part(bytes, 0, 16),
                             offset: binary_to_integer(binary_part(bytes, 16, 8)),
                             len:    binary_to_integer(binary_part(bytes, 24, 8))}
  end
end
