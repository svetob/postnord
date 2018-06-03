defmodule Postnord.RPC.Client do
  @moduledoc """
  Behaviour for RPC client handling internal cluster requests
  """

  @callback replicate(
              pid :: atom() | pid(),
              partition :: String.t(),
              id :: String.t(),
              timestamp :: integer,
              message :: iolist(),
              timeout :: integer
            ) :: :ok | {:error, any()}

  @callback hold(
              pid :: atom() | pid(),
              partition :: String.t(),
              id :: String.t(),
              timeout :: integer
            ) :: :hold | :reject | :tombstone | {:error, any()}

  @callback tombstone(
              pid :: atom() | pid(),
              partition :: String.t(),
              id :: String.t(),
              timeout :: integer
            ) :: :ok | {:error, any()}

  @callback flush(
              pid :: atom() | pid(),
              partition :: String.t(),
              timeout :: integer
            ) :: :ok | {:error, any()}
end
