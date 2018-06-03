defmodule Postnord.Consumer.Partition.MessageLogReader do
  require Logger

  alias Postnord.Consumer.Partition.State

  @moduledoc """
  Consumer functions for opening and reading from message log.
  """

  @readahead 256 * 1024 * 1024
  @file_opts [:binary, :read, :raw, {:read_ahead, @readahead}]

  @doc """
  Ensure message log iodevice is opened.
  """
  def ensure_open(%State{messagelog_iodevice: nil} = state) do
    case File.open(state.messagelog_path, @file_opts) do
      {:ok, iodevice} ->
        Logger.debug(fn -> "Reading: #{Path.absname(state.messagelog_path)}" end)
        {:ok, %State{state | messagelog_iodevice: iodevice}}

      {:error, reason} ->
        Logger.error("Failed to open #{Path.absname(state.messagelog_path)}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def ensure_open(state) do
    {:ok, state}
  end

  @doc """
  Read message from message log.
  """
  def read(state, entry) do
    iodevice = state.messagelog_iodevice
    offset = entry.offset
    size = entry.len

    case :file.pread(iodevice, offset, size) do
      {:ok, bytes} when byte_size(bytes) == size ->
        {:ok, state, entry.id, bytes}

      {:ok, bytes} ->
        reason = "Message size #{byte_size(bytes)} did not match expected size #{size}"
        Logger.error("#{__MODULE__} #{reason}")
        {:error, reason}

      :eof ->
        Logger.error("Failed to read message log entry: EOF")
        {:error, :eof}

      {:error, reason} ->
        Logger.error("Failed to read message log entry: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
