defmodule Postnord.RPC.Client do
  @moduledoc """
  Behaviour for RPC client handling internal cluster requests
  """

  @callback replicate(atom() | pid(), String.t(), String.t(), integer, iolist(), integer) ::
              :ok | {:error, any()}

  @callback tombstone(atom() | pid(), String.t(), String.t(), integer) :: :ok | {:error, any()}

  @callback flush(atom() | pid(), String.t(), integer) :: :ok | {:error, any()}
end
