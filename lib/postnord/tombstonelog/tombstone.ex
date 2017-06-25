defmodule Postnord.TombstoneLog.Tombstone do
  @moduledoc """
  Tombstone definition.
  """

  defstruct id: 0

  @byte_size 16
  def byte_size, do: @byte_size

  def as_bytes(tombstone) do
    tombstone.id
  end

  def from_bytes(bytes) do
    # TODO Assert bytes size
    %Postnord.TombstoneLog.Tombstone{id: bytes}
  end
end
