defmodule Postnord.Consumer.PartitionConsumer.IndexLog do
  require Logger

  alias Postnord.IndexLog.Entry
  alias Postnord.Consumer.PartitionConsumer.State
  alias Postnord.TombstoneLog.Tombstone

  @moduledoc """
  Consumer functions for opening and reading from index log.
  """

  @file_opts [:read, :raw, :binary]

  def ensure_open_indexlog({:ok, %State{indexlog_iodevice: nil} = state})  do
    case File.open(state.indexlog_path, @file_opts)  do
      {:ok, iodevice} ->
        Logger.info "Reading: #{Path.absname(state.indexlog_path)}"
        {:ok, %State{state | indexlog_iodevice: iodevice}}
      {:error, reason} ->
        Logger.error("Failed to open #{Path.absname(state.indexlog_path)}: #{inspect reason}")
        {:error, reason}
    end
  end
  def ensure_open_indexlog({:ok, _state} = ok), do: ok
  def ensure_open_indexlog({:error, _reason} = error), do: error

  def next_index_entry({:ok, state}) do
    scan_index_entries(state)
  end
  def next_index_entry(:empty), do: :empty
  def next_index_entry({:error, _reason} = error), do: error

  # Scan index entries until one which is not tombstoned is found
  # TODO: This is a first dumb+wrong implementation, just to get started.
  #       It will resend a message until it is accepted, and not resend earlier
  #       requeued entries.
  defp scan_index_entries(state) do
    case :file.pread(state.indexlog_iodevice, state.indexlog_bytes_read, Entry.byte_size) do
      :eof -> :empty
      {:error, reason} ->
        Logger.error("Failed to read index log entry: #{inspect reason}")
        {:error, reason}
      {:ok, bytes} ->
        entry = Entry.from_bytes(bytes)
        if state.tombstones |> MapSet.member?(tombstone(entry)) do
          # Message is tombstoned, proceed
          state = %State{state | indexlog_bytes_read: state.indexlog_bytes_read + Entry.byte_size}
          scan_index_entries(state)
        else
          # Message not tombstoned, read it
          {:ok, state, entry}
        end
    end
  end

  defp tombstone(entry), do: %Tombstone{id: entry.id}
end
