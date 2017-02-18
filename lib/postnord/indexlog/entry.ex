defmodule Postnord.IndexLog.Entry do
  require Logger

  @moduledoc """
  Index log entry definition.

  An entry contains the ID, size, and disk location of a single message.
  """
  # TODO: Improve storage format

  defstruct id: 0, offset: 0, len: 0

  @byte_size 24
  def byte_size, do: @byte_size

  def as_bytes(entry) do
    to_binary(entry.id, 8) <> to_binary(entry.offset, 8) <> to_binary(entry.len, 8)
  end

  def from_bytes(bytes) do
    # TODO Assert bytes size
    %Postnord.IndexLog.Entry{id:     from_binary(binary_part(bytes, 0, 8)),
                             offset: from_binary(binary_part(bytes, 8, 8)),
                             len:    from_binary(binary_part(bytes, 16, 8))}
  end

  defp to_binary(value, bytes) do
    digits = Integer.digits(value, 256)
    padded = List.duplicate(0, bytes - length(digits)) ++ digits
    :erlang.iolist_to_binary(padded)
  end

  defp from_binary(value) do
    Integer.undigits(:erlang.binary_to_list(value), 256)
  end
end
