defmodule Postnord.Consumer.Partition.IndexLog do
  require Logger

  alias Postnord.IndexLog.Entry
  alias Postnord.Consumer.Partition.State

  @moduledoc """
  Consumer functions for opening and reading from index log.
  """

  @file_opts [:read, :raw, :binary]

  def ensure_open_indexlog({:ok, %State{indexlog_iodevice: nil} = state})  do
    case File.open(state.indexlog_path, @file_opts)  do
      {:ok, iodevice} ->
        Logger.debug "Reading: #{Path.absname(state.indexlog_path)}"
        {:ok, %State{state | indexlog_iodevice: iodevice}}
      {:error, reason} ->
        Logger.error("Failed to open #{Path.absname(state.indexlog_path)}: #{inspect reason}")
        {:error, reason}
    end
  end
  def ensure_open_indexlog({:ok, state} = ok), do: ok
  def ensure_open_indexlog({:error, reason} = error), do: error

  def next_index_entry({:ok, state}) do
    case :file.pread(state.indexlog_iodevice, state.indexlog_bytes_read, Entry.byte_size) do
      :eof -> :empty
      {:error, reason} ->
        Logger.error("Failed to read index log entry: #{inspect reason}")
        {:error, reason}
      {:ok, bytes} ->
        state = %State{state | indexlog_bytes_read: state.indexlog_bytes_read + Entry.byte_size}
        entry = Entry.from_bytes(bytes)
        {:ok, state, entry}
    end
  end
  def next_index_entry(:empty), do: :empty
  def next_index_entry({:error, reason} = error), do: error

end
