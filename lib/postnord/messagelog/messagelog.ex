defmodule Postnord.MessageLog.State do
  defstruct path: "",
            iodevice: nil,
            offset: 0,
            buffer: <<>>,
            buffer_size: 0,
            flush_timeout: 10,
            callbacks: []
end

defmodule Postnord.MessageLog do
  require Logger
  use GenServer
  alias Postnord.MessageLog.State, as: State

  @moduledoc """
  Writes messages to a message log.

  state :: {file, offset, buffer, callback}

  file :: append-only message log file iodevice
  offset :: current file offset
  buffer :: data buffer to write
  callback :: callback processes to notify on write

  TODO: Flush at regular intervals
  TODO: close(pid)
  """

  @file_opts [:binary, :append]

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(state) do
    # Create output directory
    :ok = state.path
    |> Path.dirname()
    |> File.mkdir_p()

    # Open output file
    Logger.info "Appending to: #{Path.absname(state.path)}"
    file = File.open!(state.path, @file_opts)

    {:ok, %State{state | iodevice: file}}
  end

  @doc """
  Write a message to the log
  """
  def write(pid, bytes, metadata) do
    GenServer.cast(pid, {:write, self(), bytes, metadata})
  end

  def handle_cast({:write, from, bytes, metadata}, state) do
    state = buffer(state, from, bytes, metadata)

    if byte_size(state.buffer) >= state.buffer_size do
      {:noreply, flush(state)}
    else
      {:noreply, state, state.flush_timeout}
    end
  end

  @doc """
  Flushes buffer to disk if process receives no messages within flush_timeout ms
  """
  def handle_info(:timeout, state) do
    {:noreply, flush(state)}
  end

  @doc """
  Buffer incoming data for the next write, and add the `from` process to the
  callbacks list.
  """
  defp buffer(state, from, bytes, metadata) do
    len = byte_size(bytes)
    %State{state | offset: state.offset + len,
                   buffer: state.buffer <> bytes,
                   callbacks: [{from, state.offset, len, metadata} | state.callbacks]}
  end

  @doc """
  Persist the write buffer to disk and notify all in callbacks list.
  """
  defp flush(state) do
    spawn fn ->
      Logger.debug "Flushing messagelog buffer"
      case IO.binwrite(state.iodevice, state.buffer) do
        :ok ->
          Logger.debug "Flushing messagelog buffer :ok"
          state.callbacks |> Enum.each(fn {from, offset, len, metadata} ->
            GenServer.cast(from, {:write_messagelog_ok, offset, len, metadata})
          end)
        {:error, reason} ->
          #TODO
          Logger.error("Failed writing to message log: #{inspect reason}")
      end
    end
    %State{state | buffer: <<>>, callbacks: []}
  end
end
