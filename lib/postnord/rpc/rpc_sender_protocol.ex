defmodule Postnord.RPC.Sender do
  @callback replicate(atom() | pid(), String.t(), String.t(), iolist(), integer) :: :ok | {:error, any()}
  @callback tombstone(atom() | pid(), String.t(), String.t(), integer) :: :ok | {:error, any()}
end
