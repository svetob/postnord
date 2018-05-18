defmodule Postnord.RPC.Client.Local do
  @behaviour Postnord.RPC.Client

  alias Postnord.Partition
  alias Postnord.Consumer.PartitionConsumer

  @moduledoc """
  RPC sender handling local RPC invocations.
  """

  def replicate(_pid, _partition, id, timestamp, message, timeout \\ 5_000) do
    Partition.replicate_message(Partition, id, timestamp, message, timeout)
  end

  def tombstone(_pid, _partition, id, timeout \\ 5_000) do
    case PartitionConsumer.accept(PartitionConsumer, id, timeout) do
      :ok -> :ok
      :noop -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, other}
    end
  end

  def flush(_pid, _queue, timeout \\ 5_000) do
    PartitionConsumer.flush(PartitionConsumer, timeout)
  end
end
