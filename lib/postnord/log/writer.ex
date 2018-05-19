defmodule Postnord.Log.Writer.State do
  @moduledoc """
  State struct for log writer.
  """

  defstruct path: "",
            iodevice: nil,
            offset: 0,
            buffer: <<>>,
            buffer_size: 0,
            flush_timeout: 10,
            callbacks: []
end

defmodule Postnord.Log.Writer do
  use GenServer

  require Logger

  alias Postnord.Log.Writer.State

  @moduledoc """
  Configurable log writer for byte entries of any size.

  Writes byte entries in received order. Will buffer incoming entries until
  either buffer_size or flush_timeout is reached, and then persisting the buffer
  to disk. Response is sent to callers once entry is persisted.
  """

  @file_opts [:binary, :append, :sync]

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(%State{path: path} = state) do
    # Create output directory
    :ok =
      path
      |> Path.dirname()
      |> File.mkdir_p()

    # Open output file
    Logger.debug(fn -> "Opening log file: #{Path.absname(path)}" end)
    iodevice = File.open!(path, @file_opts)
    iostat = File.stat!(path)
    offset = iostat.size

    {:ok, %State{state | iodevice: iodevice, offset: offset}}
  end

  @doc """
  Write bytes to the log. Returns offset and length of the written entry.
  """
  @spec write(pid, binary) :: {:ok, integer(), integer()} | {:error, term()}
  def write(pid, bytes) do
    GenServer.call(pid, {:write, bytes})
  end

  def handle_call({:write, bytes}, from, state) do
    state = buffer(state, from, bytes)

    Logger.debug(fn -> "#{__MODULE__} Buffer size is #{byte_size(state.buffer)}" end)
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

  def terminate(reason, %State{path: path}) do
    Logger.debug(fn -> "Log #{path} terminating: #{inspect(reason)}" end)
  end

  # Buffer incoming data for the next write, and add the `from` process to the
  # callbacks list.
  defp buffer(%State{offset: offset, buffer: buffer, callbacks: callbacks} = state, from, bytes) do
    len = byte_size(bytes)

    %State{
      state
      | offset: offset + len,
        buffer: buffer <> bytes,
        callbacks: [{from, offset, len} | callbacks]
    }
  end

  # Persist the write buffer to disk and notify all in callbacks list.
  defp flush(state) do
    spawn(fn ->
      Logger.debug(fn -> "#{__MODULE__} Flushing data to #{state.path}" end)

      state.iodevice
      |> IO.binwrite(state.buffer)
      |> send_callbacks(state.callbacks)
    end)

    %State{state | buffer: <<>>, callbacks: []}
  end

  defp send_callbacks(:ok, callbacks) do
    callbacks
    |> Enum.each(fn {from, offset, len} ->
      GenServer.reply(from, {:ok, offset, len})
    end)
  end

  defp send_callbacks({:error, reason}, callbacks) do
    Logger.error("#{__MODULE__} Failed writing to index log: #{inspect(reason)}")

    callbacks
    |> Enum.each(fn {from, _offset, _len} ->
      GenServer.reply(from, {:error, reason})
    end)
  end
end
