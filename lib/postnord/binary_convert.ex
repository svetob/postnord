defmodule Postnord.BinaryConvert do

  @moduledoc """
  Convert integer values to/from binaries
  """

  def integer_to_binary(value, bytes) do
    iolist = case Integer.digits(value, 256) do
      digits when length(digits) < bytes ->
        List.duplicate(0, bytes - length(digits)) ++ digits
      digits when length(digits) > bytes ->
        raise "Value too large to fit in #{bytes} bytes: #{value}"
      digits ->
        digits
    end
    :erlang.iolist_to_binary(iolist)
  end

  def binary_to_integer(binary) do
    Integer.undigits(:erlang.binary_to_list(binary), 256)
  end
end
