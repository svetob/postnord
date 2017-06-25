defmodule Postnord.IndexLog.State do
  @moduledoc """
  State struct for index log.
  """

  defstruct path: "",
            iodevice: nil,
            buffer: <<>>,
            buffer_size: 0,
            flush_timeout: 10,
            callbacks: []
end

defmodule Postnord.IndexLog do
  require Logger
  use GenServer
  alias Postnord.IndexLog.Entry, as: Entry
  alias Postnord.IndexLog.State, as: State

  @moduledoc """
  Appends message indexes to the index log

  state :: {file, offset, buffer, callback}

  file :: append-only message log file iodevice
  buffer :: data buffer to write
  callback :: callback processes to notify on write
  """

  @file_opts [:append]

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(state) do
    # Create output directory
    :ok = state.path
    |> Path.dirname()
    |> File.mkdir_p()

    # Open output file
    Logger.info "Opening: #{Path.absname(state.path)}"
    file = File.open!(state.path, @file_opts)

    {:ok, %State{state | iodevice: file}}
  end

  @doc """
  Write a message index to the log
  """
  def write(pid, entry) do
    GenServer.call(pid, {:write, entry})
  end

  def handle_call({:write, entry}, from, state) do
    state = buffer(state, from, entry)

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

  # Buffer incoming data for the next write, and add the `from` process to the
  # callbacks list.
  defp buffer(state, from, entry) do
    %State{state | callbacks: [from | state.callbacks],
                   buffer: state.buffer <> Entry.as_bytes(entry)}
  end

  # Persist the write buffer to disk and notify all in callbacks list.
  defp flush(state) do
    spawn fn ->
      state.iodevice
      |> IO.binwrite(state.buffer)
      |> send_callbacks(state.callbacks)
    end
    %State{state | buffer: <<>>, callbacks: []}
  end

  defp send_callbacks(:ok, callbacks) do
    callbacks |> Enum.each(fn from ->
      GenServer.reply(from, :ok)
    end)
  end
  defp send_callbacks({:error, reason}, callbacks) do
    Logger.error("Failed writing to index log: #{inspect reason}")
    callbacks |> Enum.each(fn from ->
      GenServer.reply(from, {:error, reason})
    end)
  end
end
