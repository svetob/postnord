defmodule Postnord.Partition do
  require Logger
  use GenServer

  alias Postnord.MessageLog
  alias Postnord.IndexLog
  alias Postnord.IndexLog.Entry
  alias Postnord.Consumer.PartitionConsumer

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
    %PartitionConsumer.State{path: path}
  end

  @doc """
  Replicate a single message to this partition on this node.
  """
  def replicate_message(pid, id, bytes, timeout \\ 5_000) do
    try do
      GenServer.call(pid, {:replicate, id, bytes}, timeout)
    catch
      :exit, reason ->
        Logger.error "Replication failed: #{inspect reason}"
        {:error, reason}
    end
  end

  def handle_call({:replicate, id, bytes}, from, nil) do
    spawn fn ->
      write_to_logs(id, bytes)
      GenServer.reply(from, :ok)
    end
    {:noreply, nil}
  end

  defp write_to_logs(id, bytes) do
    {:ok, offset, len} = MessageLog.write(MessageLog, bytes)
    :ok = IndexLog.write(IndexLog, %Entry{id: id, offset: offset, len: len})
  end
end
