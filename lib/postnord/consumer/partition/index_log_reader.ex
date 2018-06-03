defmodule Postnord.Consumer.Partition.IndexLogReader do
  require Logger

  alias Postnord.Partition.MessageIndex
  alias Postnord.Consumer.Partition.State
  alias Postnord.Consumer.Partition.Tombstone
  alias Postnord.Consumer.Partition.Holds

  @moduledoc """
  Consumer functions for opening and reading from index log.
  """

  @readahead 64 * 1024 * 1024
  @file_opts [:binary, :read, :raw, {:read_ahead, @readahead}]

  @doc """
  Ensure index log iodevice is opened.
  """
  def ensure_open(%State{indexlog_iodevice: nil} = state) do
    case File.open(state.indexlog_path, @file_opts) do
      {:ok, iodevice} ->
        Logger.info("Reading: #{Path.absname(state.indexlog_path)}")
        {:ok, %State{state | indexlog_iodevice: iodevice}}

      {:error, reason} ->
        Logger.error("Failed to open #{Path.absname(state.indexlog_path)}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def ensure_open(state) do
    {:ok, state}
  end

  @doc """
  Read next index entry from index log.
  """
  # Scans index entries until one which is not tombstoned is found
  # TODO: This is a first dumb+wrong implementation, just to get started.
  #       It will resend a message until it is accepted, and not resend rejected
  #       messages.
  def next(state) do
    iodevice = state.indexlog_iodevice
    offset = state.indexlog_bytes_read
    size = MessageIndex.entry_size()

    case :file.pread(iodevice, offset, size) do
      :eof ->
        :empty

      {:error, reason} ->
        Logger.error("Failed to read index log entry: #{inspect(reason)}")
        {:error, reason}

      {:ok, bytes} ->
        entry = MessageIndex.from_bytes(bytes)

        if consumed?(entry, state) do
          # Message is consumed, proceed
          next(%State{state | indexlog_bytes_read: offset + size})
        else
          # Message not tombstoned, read it
          {:ok, state, entry}
        end
    end
  end

  # TODO Consumption check should check tombstones and holds, not timestamps
  defp consumed?(entry, state) do
    Holds.held?(state.holds, entry.id) or state.tombstones |> MapSet.member?(tombstone(entry))
  end

  defp tombstone(entry), do: %Tombstone{id: entry.id}
end
