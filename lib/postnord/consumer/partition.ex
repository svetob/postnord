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
            indexlog_bytes_read: 0,
            tombstones: MapSet.new(),
            timestamp_cutoff: 0
end

defmodule Postnord.Consumer.Partition do
  use GenServer

  require Logger

  alias Postnord.Consumer.Partition.{IndexLogReader, MessageLogReader, State, Tombstone}

  @moduledoc """
  This is a first pass at a consumer process to read from a partition.

  When asked for a document, the consumer will:
  [X] Open file handle if not open and files exist
  [ ] Check if the size of the files on disk for changes
  [X] Check if index log contains another unread entry
    [X] If not, return {:empty}
  [X] Attempt to read an entry from the index log
  [ ] Check if message log contains enough bytes to serve offset+len for entry
  [X] Attempt to read message bytes

  [X] If all steps above succeed, increment the entries-read counter and return {:ok, bytes}

  [ ] If any step fails, close file handles and return {:error, reason}

  Design decisions:
  - reader should rely on OS-level page cache and read-ahead for speed
  - Single reader process per partition

  Next steps:
  - Write index tombstones to tombstone log
  - Keep tombstone bloomfilter in memory
  - ACCEPT / REJECT of messages
  """

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(state) do
    {:ok,
     %State{
       state
       | messagelog_path: Path.join(state.path, "message.log"),
         indexlog_path: Path.join(state.path, "index.log")
     }}
  end

  @doc """
  Request the next message from the message log form this consumer.
  """
  @spec read(pid(), integer) :: {:ok, iolist(), iolist()} | :empty | {:error, any()}
  def read(pid, timeout \\ 5_000) do
    GenServer.call(pid, :read, timeout)
  end

  @doc """
  Accept message by id for this consumer.
  """
  @spec accept(pid(), iolist(), integer) :: :ok | :noop | {:error, any()}
  def accept(pid, id, timeout \\ 5_000) do
    GenServer.call(pid, {:accept, id}, timeout)
  end

  @doc """
  Flush queue and tombstone all current messages for this consumer.
  """
  @spec flush(pid(), integer) :: :ok | {:error, any()}
  def flush(pid, timeout \\ 5_000) do
    GenServer.call(pid, {:flush}, timeout)
  end

  def handle_call(:read, _from, state) do
    case state |> ensure_open |> read_next_message do
      {:ok, state, id, bytes} ->
        {:reply, {:ok, id, bytes}, state}

      response ->
        {:reply, response, state}
    end
  end

  def handle_call({:accept, id}, _from, state) do
    tombstone = %Tombstone{id: id}

    if tombstoned?(state, tombstone) do
      {:reply, :noop, state}
    else
      tombstones = state.tombstones |> MapSet.put(tombstone)
      {:reply, :ok, %State{state | tombstones: tombstones}}
    end
  end

  def handle_call({:flush}, _from, state) do
    {:reply, :ok, %State{state | timestamp_cutoff: Postnord.now(:nanosecond)}}
  end

  defp tombstoned?(%State{tombstones: tombstones}, tombstone) do
    MapSet.member?(tombstones, tombstone)
  end

  defp ensure_open(state) do
    case IndexLogReader.ensure_open(state) do
      {:ok, state} ->
        MessageLogReader.ensure_open(state)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_next_message({:ok, state}) do
    case IndexLogReader.next(state) do
      {:ok, state, entry} ->
        MessageLogReader.read(state, entry)

      other ->
        other
    end
  end

  defp read_next_message({:error, reason}) do
    {:error, reason}
  end
end
