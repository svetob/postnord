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

  # TODO: Make this not a genserver
  # TODO: Instead of timeout, check for empty message queue

  @file_opts [:binary, :append, :raw, :sync]

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
    Logger.debug(fn -> "#{__MODULE__} Flushing data to #{state.path}" end)

    case do_buffer_write(state.iodevice, state.buffer) do
      :ok ->
        reply_ok(state.callbacks)

      {:error, reason} ->
        Logger.error("#{__MODULE__} Failed writing to index log: #{inspect(reason)}")
        reply_error(reason, state.callbacks)
    end

    %State{state | buffer: <<>>, callbacks: []}
  end

  defp do_buffer_write(iodevice, buffer) do
    Logger.debug(fn -> "#{__MODULE__} Writing data" end)
    res = :file.write(iodevice, buffer)
    Logger.debug(fn -> "#{__MODULE__} Wrote data" end)
    res
  end

  defp reply_ok(callbacks) do
    Enum.each(callbacks, fn {from, offset, len} ->
      GenServer.reply(from, {:ok, offset, len})
    end)
  end

  defp reply_error(reason, callbacks) do
    Enum.each(callbacks, fn {from, _offset, _len} ->
      GenServer.reply(from, {:error, reason})
    end)
  end
end
