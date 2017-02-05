defmodule Postnord.IndexLog do
  require Logger
  use GenServer
  alias Postnord.IndexLog.Entry, as: Entry

  @moduledoc """
  Appends message indexes to the index log

  state :: {file, offset, buffer, pending}

  file :: append-only message log file iodevice
  buffer :: data buffer to write
  respond :: responses to send on write
  """

  @file_opts [:binary, :append]
  @buffer_size 0 #8192

  def start_link(path, opts \\ []) do
    GenServer.start_link(__MODULE__, path, opts)
  end

  def init(path) do
    # Open output file
    path_abs = Path.absname(path)
    file = File.open!(path_abs, @file_opts)
    Logger.info "Writing: #{path_abs}"

    {:ok, {file, <<>>, []}}
  end

  @doc """
  Write a message index to the log
  """
  def write(pid, from, entry, metadata) do
    GenServer.cast(pid, {:write, from, entry, metadata})
  end

  def handle_cast({:write, from, entry, metadata},
                  {file, buffer, pending}) do

    pending = [{from, metadata} | pending]
    buffer = buffer <> Entry.as_bytes(entry)

    if byte_size(buffer) >= @buffer_size do
      # Flush buffer
      flush(file, buffer, pending)
      {:noreply, {file, <<>>, []}}
    else
      # Buffer data
      {:noreply, {file, buffer, pending}}
    end
  end

  defp flush(file, buffer, pending) do
    spawn fn ->
      :ok = IO.binwrite(file, buffer)
      pending |> Enum.each(fn {from, metadata} ->
        GenServer.cast(from, {:write_indexlog_ok, metadata})
      end)
    end
  end
end
