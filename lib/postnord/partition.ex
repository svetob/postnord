defmodule Postnord.Partition do
  require Logger
  use GenServer

  alias Postnord.IdGen
  alias Postnord.MessageLog
  alias Postnord.IndexLog
  alias Postnord.IndexLog.Entry
  alias Postnord.Consumer.Partition, as: PartitionConsumer

  @moduledoc """
  Managing GenServer for a single message queue partition.

  Supervises child processes for partition, distributes messages to them.
  """

  def start_link(path, opts \\ []) do
    GenServer.start_link(__MODULE__, path, opts)
  end

  def init(path) do
    import Supervisor.Spec, warn: false

    children = [
      worker(MessageLog, [message_log_state(path), [name: MessageLog]]),
      worker(IndexLog, [index_log_state(path), [name: IndexLog]]),
      worker(PartitionConsumer, [partition_reader_state(path), [name: PartitionConsumer]])
    ]

    Supervisor.start_link(children, [strategy: :one_for_one])
    {:ok, nil}
  end

  defp message_log_state(path) do
    env = Application.get_env(:postnord, MessageLog, [])
    %Postnord.MessageLog.State{
      buffer_size: env |> Keyword.get(:buffer_size),
      flush_timeout: env |> Keyword.get(:flush_timeout),
      path: path
    }
  end

  defp index_log_state(path) do
    env = Application.get_env(:postnord, IndexLog, [])
    %Postnord.IndexLog.State{
      buffer_size: env |> Keyword.get(:buffer_size),
      flush_timeout: env |> Keyword.get(:flush_timeout),
      path: Path.join(path, "index.log")
    }
  end

  defp partition_reader_state(path) do
    %Postnord.Consumer.Partition.State{path: path}
  end

  @doc """
  Writes a single message to the partition.
  """
  def write_message(pid, bytes, timeout \\ 5_000) do
    GenServer.call(pid, {:write, bytes}, timeout) # FIXME timeout is not triggering
  end

  def handle_call({:write, bytes}, from, nil) do
    id = Postnord.IdGen.id()
    MessageLog.write(MessageLog, bytes, {from, id})
    {:noreply, nil}
  end

  def handle_cast({:write_messagelog_ok, offset, len, {from, id}}, nil) do
    entry = %Entry{id: id, offset: offset, len: len}
    IndexLog.write(IndexLog, entry, {from})
    {:noreply, nil}
  end

  def handle_cast({:write_indexlog_ok, {from}}, nil) do
    GenServer.reply(from, :ok)
    {:noreply, nil}
  end
end
