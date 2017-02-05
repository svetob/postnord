defmodule Postnord.MessageLog do
  require Logger
  use GenServer

  @moduledoc """
  Writes messages to a message log.

  state :: {file, offset, buffer, pending}

  file :: append-only message log file iodevice
  offset :: current file offset
  buffer :: data buffer to write
  respond :: responses to send on write

  TODO: Flush at regular intervals
  TODO: close(pid)
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

    {:ok, {file, 0, <<>>, []}}
  end

  @doc """
  Write a message to the log
  """
  def write(pid, from, bytes, metadata) do
    GenServer.cast(pid, {:write, from, bytes, metadata})
  end

  def handle_cast({:write, from, bytes, metadata},
                  {file, offset, buffer, pending}) do

    len = byte_size(bytes)
    pending = [{from, offset, len, metadata} | pending]
    offset = offset + len
    buffer = buffer <> bytes

    if byte_size(buffer) >= @buffer_size do
      # Flush buffer
      flush(file, buffer, pending)
      {:noreply, {file, offset, <<>>, []}}
    else
      # Buffer data
      {:noreply, {file, offset, buffer, pending}}
    end
  end

  defp flush(file, buffer, pending) do
    spawn fn ->
      :ok = IO.binwrite(file, buffer)
      pending |> Enum.each(fn {from, offset, len, metadata} ->
        GenServer.cast(from, {:write_messagelog_ok, offset, len, metadata})
      end)
    end
  end
end
