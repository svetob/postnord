defmodule Postnord.IndexLog.Entry do
  defstruct id: 0, offset: 0, len: 0

  def as_bytes(entry) do
    to_binary(entry.id, 8) <> to_binary(entry.offset, 4) <> to_binary(entry.len, 4)
  end

  defp to_binary(value, bytes) do
    digits = Integer.digits(value, 256)
    padded = List.duplicate(0, bytes-length(digits)) ++ digits
    :erlang.iolist_to_binary(padded)
  end

  # defp from_binary(value) do
  #   Integer.undigits(:erlang.binary_to_list(value), 256)
  # end
end
