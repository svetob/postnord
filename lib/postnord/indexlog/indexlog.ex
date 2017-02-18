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
    Logger.info "Opening: #{Path.absname(state.path)}"
    file = File.open!(state.path, @file_opts)

    {:ok, %State{state | iodevice: file}}
  end

  @doc """
  Write a message index to the log
  """
  def write(pid, entry, metadata) do
    GenServer.cast(pid, {:write, self(), entry, metadata})
  end

  def handle_cast({:write, from, entry, metadata}, state) do

    state = buffer(state, from, entry, metadata)

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
  defp buffer(state, from, entry, metadata) do
    %State{state | callbacks: [{from, metadata} | state.callbacks],
                   buffer: state.buffer <> Entry.as_bytes(entry)}
  end

  @doc """
  Persist the write buffer to disk and notify all in callbacks list.
  """
  defp flush(state) do
    spawn fn ->
      case IO.binwrite(state.iodevice, state.buffer) do
        :ok -> send_callbacks(state.callbacks)
        {:error, reason} ->
          # TODO Handle error when persisting
          Logger.error("Failed writing to index log: #{inspect reason}")
      end
    end
    %State{state | buffer: <<>>, callbacks: []}
  end

  defp send_callbacks(callbacks) do
    callbacks |> Enum.each(fn {from, metadata} ->
      GenServer.cast(from, {:write_indexlog_ok, metadata})
    end)
  end
end
