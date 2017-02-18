defmodule Postnord.Consumer.Partition.State do
  @moduledoc """
  State struct for partition reader.
  """

  defstruct path: "",
            entries_read: 0,
            messagelog_path: "",
            messagelog_iodevice: nil,
            indexlog_path: "",
            indexlog_iodevice: nil,
            indexlog_bytes_read: 0
end

defmodule Postnord.Consumer.Partition do
  require Logger
  import Postnord.Consumer.Partition.IndexLog
  import Postnord.Consumer.Partition.MessageLog
  use GenServer

  alias Postnord.Consumer.Partition.State

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
  - reader should rely on OS-level page cache and read-ahead for speed
  - Single reader process per partition

  Next steps:
  - Write index tombstones to tombstone log
  - Keep tombstone bloomfilter in memory
  - ACCEPT / REJECT / REQUEUE of messages
  """

  @file_opts [:read, :raw, :binary]

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(state) do
    {:ok, %State{state | messagelog_path: Path.join(state.path, "message.log"),
                         indexlog_path: Path.join(state.path, "index.log")}}
  end

  def read(pid) do
    GenServer.call(pid, :read)
  end

  def handle_call(:read, from, state) do
    case state |> ensure_open |> next_index_entry |> read_message do
      :empty -> {:reply, :empty, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
      {:ok, state, bytes} -> {:reply, {:ok, bytes}, state}
    end
  end

  defp ensure_open(state) do
    {:ok, state}
    |> ensure_open_indexlog
    |> ensure_open_messagelog
  end
end
