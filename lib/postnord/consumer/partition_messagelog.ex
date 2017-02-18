defmodule Postnord.Consumer.Partition.MessageLog do
  require Logger

  alias Postnord.Consumer.Partition.State

  @moduledoc """
  Consumer functions for opening and reading from message log.
  """

  @file_opts [:read, :raw, :binary]

  def ensure_open_messagelog({:ok, %State{messagelog_iodevice: nil} = state})  do
    case File.open(state.messagelog_path, @file_opts)  do
      {:ok, iodevice} ->
        Logger.debug "Reading: #{Path.absname(state.messagelog_path)}"
        {:ok, %State{state | messagelog_iodevice: iodevice}}
      {:error, reason} ->
        Logger.error("Failed to open #{Path.absname(state.messagelog_path)}: #{inspect reason}")
        {:error, reason}
    end
  end
  def ensure_open_messagelog({:ok, state} = ok), do: ok
  def ensure_open_messagelog({:error, reason} = error), do: error

  def read_message({:ok, state, entry}) do
    len = entry.len
    case :file.pread(state.messagelog_iodevice, entry.offset, len) do
      :eof -> :empty
      {:error, reason} ->
        Logger.error("Failed to read message log entry: #{inspect reason}")
        {:error, reason}
      {:ok, bytes} when byte_size(bytes) != len ->
        Logger.warn("Message size #{byte_size bytes} did not match expected size #{len}, assuming message is not fully written to disk yet")
        :empty
      {:ok, bytes}  -> {:ok, state, bytes}
    end
  end
  def read_message(:empty), do: :empty
  def read_message({:error, reason} = error), do: error
end
