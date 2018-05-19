defmodule Postnord.Partition do
  require Logger
  use GenServer

  alias Postnord.Log.Writer, as: LogWriter
  alias Postnord.Partition.MessageIndex
  alias Postnord.Consumer

  @moduledoc """
  Managing GenServer for a single message queue partition.

  Supervises child processes for partition, distributes messages to them.
  """

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init({path, queue, partition_id}) do
    import Supervisor.Spec, warn: false

    data_path = path |> Path.join(queue) |> Path.join(partition_id)

    children = [
      %{
        id: :message_log,
        start: {LogWriter, :start_link, [message_log_state(data_path), [name: :message_log]]}
      },
      %{
        id: :index_log,
        start: {LogWriter, :start_link, [index_log_state(data_path), [name: :index_log]]}
      },
      %{
        id: Consumer.Partition,
        start: {Consumer.Partition, :start_link, [partition_reader_state(data_path), [name: Consumer.Partition]]}
      }
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
    {:ok, nil}
  end

  defp message_log_state(path) do
    env = Application.get_env(:postnord, :message_log, [])

    %LogWriter.State{
      buffer_size: Keyword.get(env, :buffer_size),
      flush_timeout: Keyword.get(env, :flush_timeout),
      path: Path.join(path, "message.log")
    }
  end

  defp index_log_state(path) do
    env = Application.get_env(:postnord, :index_log, [])

    %LogWriter.State{
      buffer_size: Keyword.get(env, :buffer_size),
      flush_timeout: Keyword.get(env, :flush_timeout),
      path: Path.join(path, "index.log")
    }
  end

  defp partition_reader_state(path) do
    %Consumer.Partition.State{path: path}
  end

  @doc """
  Replicate a single message to this partition on this node.
  TODO Somehow ensure message does not already exist locally.
  """
  def replicate_message(pid, id, timestamp, bytes, timeout \\ 5_000) do
    GenServer.call(pid, {:replicate, id, timestamp, bytes}, timeout)
  end

  def handle_call({:replicate, id, timestamp, bytes}, from, nil) do
    spawn(fn ->
      GenServer.reply(from, write_to_logs(id, timestamp, bytes))
    end)

    {:noreply, nil}
  end

  defp write_to_logs(id, timestamp, bytes) do
    case LogWriter.write(:message_log, bytes) do
      {:ok, offset, len} ->
        index = %MessageIndex{id: id, timestamp: timestamp, offset: offset, len: len}
        index_bytes = MessageIndex.as_bytes(index)

        case LogWriter.write(:index_log, index_bytes) do
          {:ok, _, _} ->
            :ok

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end
