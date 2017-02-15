defmodule Postnord.Reader.Partition.State do
  defstruct path: "",
            entries_read: 0,
            messagelog_path: "",
            messagelog_iodevice: nil,
            indexlog_path: "",
            indexlog_iodevice: nil,
            indexlog_bytes_read: 0
end

defmodule Postnord.Reader.Partition do
  require Logger
  use GenServer

  alias Postnord.Reader.Partition.State
  alias Postnord.IndexLog.Entry

  @moduledoc """
  This is a first pass at a consumer process to read from a partition.

  When asked for a document, the consumer will:
  - Open file handle if not open and files exist
  - Check if the size of the files on disk for changes
  - Check if index log contains another unread entry
    - If not, return {:empty}
  - Attempt to read an entry from the index log
  - Check if message log contains enough bytes to serve offset+len for entry
  - Attempt to read message bytes

  If all steps above succeed, increment the entries-read counter and return {:ok, bytes}

  If any step fails, close file handles and return {:error, reason}

  Design decisions:
  - index/message reader should rely on pagecache and readahead for speed
  - Single reader process per partition

  Next steps:
  - Write index tombstones to tombstone log
  - Keep tombstone bloomfilter in memory
  - ACCEPT / REJECT / REQUEUE of messages
  """

  @file_opts [:read]
  @entry_size 24 # TODO Move to a better place

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(state) do
    {:ok, %State{state | messagelog_path: Path.join(state.path, "message.log"),
                         indexlog_path: Path.join(state.path, "index.log")}}
  end

  @spec read(pid) :: {:ok, binary} | :empty | {:error, any}
  def read(pid) do
    GenServer.call(pid, :read)
  end

  def handle_call(:read, from, state) do
    case state |> ensure_open |> next_index |> read_entry do
      :empty -> {:reply, :empty, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
      {:ok, state, entry, bytes} -> {:reply, {:ok, bytes}, state}
    end
  end

  defp ensure_open(state), do: state |> ensure_open_indexlog |> ensure_open_messagelog

  defp ensure_open_indexlog(%State{indexlog_iodevice: nil} = state)  do
    case File.open(state.indexlog_path, @file_opts)  do
      {:ok, iodevice} ->
        Logger.debug "Reading: #{Path.absname(state.indexlog_path)}"
        {:ok, %State{state | indexlog_iodevice: iodevice}}
      {:error, reason} ->
        Logger.error("Failed to open #{Path.absname(state.indexlog_path)}: #{inspect reason}")
        {:error, reason}
    end
  end
  defp ensure_open_indexlog(state), do: {:ok, state}

  defp ensure_open_messagelog({:ok, %State{messagelog_iodevice: nil} = state})  do
    case File.open(state.messagelog_path, @file_opts)  do
      {:ok, iodevice} ->
        Logger.debug "Reading: #{Path.absname(state.messagelog_path)}"
        {:ok, %State{state | messagelog_iodevice: iodevice}}
      {:error, reason} ->
        Logger.error("Failed to open #{Path.absname(state.messagelog_path)}: #{inspect reason}")
        {:error, reason}
    end
  end
  defp ensure_open_messagelog({:ok, state}), do: {:ok, state}
  defp ensure_open_messagelog({:error, reason}), do: {:error, reason}

  @spec next_index({:ok, State.t()}) :: {:ok, State, Entry} | :empty | {:error, any()}
  defp next_index({:ok, state}) do
    case :file.pread(state.indexlog_iodevice, state.indexlog_bytes_read, @entry_size) do
      :eof -> :empty
      {:error, reason} -> {:error, reason}
      {:ok, bytes} ->
        state = %State{state | indexlog_bytes_read: state.indexlog_bytes_read + @entry_size}
        entry = Entry.from_bytes(bytes)
        {:ok, state, entry}
    end
  end
  defp next_index(:empty), do: :empty
  defp next_index({:error, reason}), do: {:error, reason}

  @spec read_entry({:ok, State.t(), Entry.t()}) :: {:ok, State.t(), Entry.t(), binary} | :empty | {:error, any()}
  defp read_entry({:ok, state, entry}) do
    len = entry.len
    case :file.pread(state.messagelog_iodevice, entry.offset, len) do
      :eof -> :empty
      {:error, reason} -> {:error, reason}
      {:ok, bytes} when byte_size(bytes) != len ->
        Logger.warn("Message size #{byte_size bytes} did not match expected size #{len}, assuming message is not fully written to disk yet")
        :empty
      {:ok, bytes}  -> {:ok, state, entry, bytes}
    end
  end
  defp read_entry(:empty), do: :empty
  defp read_entry({:error, reason}), do: {:error, reason}
end
