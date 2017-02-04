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
  @buffer_size 8192


  def start_link(path, opts \\ []) do
    GenServer.start_link(__MODULE__, path, opts)
  end

  def init(path) do
    # Open output file
    path_abs = Path.absname(path)
    Logger.info "Opening file for reading #{path_abs}"
    file = File.open!(path_abs, @file_opts)
    {:ok, {file, 0, <<>>, []}}
  end

  @doc """
  Write a message to the log
  """
  def write(pid, caller, bytes, metadata) do
    GenServer.cast(pid, {:write, caller, bytes, metadata})
  end

  def handle_cast({:write, caller, bytes, metadata},
                  {file, offset, buffer, pending}) do

    len = byte_size(bytes)
    pending = [{caller, offset, len, metadata} | pending]
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
      pending |> Enum.each(fn {caller, offset, len, metadata} ->
        GenServer.cast(caller, {:write_ok, offset, len, metadata})
      end)
    end
  end
end
