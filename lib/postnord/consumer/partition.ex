defmodule Postnord.Consumer.Partition.State do
  alias Postnord.Consumer.Partition.Holds

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
            holds: Holds.new(),
            tombstones: MapSet.new(),
            timestamp_cutoff: 0
end

defmodule Postnord.Consumer.Partition do
  use GenServer

  require Logger

  alias Postnord.Consumer.Partition.{
    IndexLogReader,
    MessageLogReader,
    State,
    Tombstone,
    Holds
  }

  @moduledoc """
  This is a first pass at a consumer process to read from a partition.

  When asked for a document, the consumer will:
  [X] Open file handle if not open and files exist
  [X] Check if index log contains another unread entry
  [X] If not, return {:empty}
  [X] Attempt to read an entry from the index log
  [X] Attempt to read message bytes, assume that message log contains it

  [X] If all steps above succeed, increment the entries-read counter and return {:ok, bytes}

  [ ] If any step fails, close file handles and return {:error, reason}

  Design decisions:
  [X] reader should rely on OS-level page cache and read-ahead for speed
  [X] Single reader process per partition

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
  Request the next message from the message log from this consumer.
  """
  @spec read(pid(), integer) :: {:ok, iolist(), iolist()} | :empty | {:error, term()}
  def read(pid, timeout \\ 5_000) do
    GenServer.call(pid, :read, timeout)
  end

  @doc """
  Request a hold of an ID. Returns :hold, :reject or :tombstone.
  """
  @spec hold(pid(), iolist(), integer) :: :hold | :reject | :tombstone | {:error, term()}
  def hold(pid, id, timeout \\ 5_000) do
    GenServer.call(pid, {:hold, id}, timeout)
  end

  @doc """
  Accept message by ID for this consumer.
  """
  @spec accept(pid(), iolist(), integer) :: :ok | :noop | {:error, term()}
  def accept(pid, id, timeout \\ 5_000) do
    GenServer.call(pid, {:accept, id}, timeout)
  end

  @doc """
  Flush queue and tombstone all current messages for this consumer.
  """
  @spec flush(pid(), integer) :: :ok | {:error, term()}
  def flush(pid, timeout \\ 5_000) do
    GenServer.call(pid, {:flush}, timeout)
  end

  def handle_call(:read, _from, state) do
    case state |> ensure_open |> read_next_message do
      {:ok, next_state, id, bytes} ->
        {:reply, {:ok, id, bytes}, hold_id(next_state, id)}

      response ->
        {:reply, response, state}
    end
  end

  def handle_call({:hold, id}, _from, state) do
    if tombstoned?(state, id) do
      {:reply, :tombstone, state}
    else
      {holds, _expired_ids} = Holds.cleanup(state.holds)

      if Holds.held?(holds, id) do
        {:reply, :reject, %State{state | holds: holds}}
      else
        {:reply, :hold, hold_id(state, id)}
      end
    end
  end

  def handle_call({:accept, id}, _from, state) do
    if tombstoned?(state, id) do
      {:reply, :noop, state}
    else
      tombstones = MapSet.put(state.tombstones, %Tombstone{id: id})
      {:reply, :ok, %State{state | tombstones: tombstones}}
    end
  end

  def handle_call({:flush}, _from, state) do
    {:reply, :ok, %State{state | timestamp_cutoff: Postnord.now(:nanosecond)}}
  end

  defp tombstoned?(%State{tombstones: tombstones}, id) do
    MapSet.member?(tombstones, %Tombstone{id: id})
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

  defp hold_id(%State{holds: holds} = state, id) do
    %State{state | holds: Holds.hold(holds, id)}
  end
end
