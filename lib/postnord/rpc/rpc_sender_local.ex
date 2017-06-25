defmodule Postnord.RPC.Client.Local do
  @behaviour Postnord.RPC.Client

  alias Postnord.Partition
  alias Postnord.Consumer.PartitionConsumer

  @moduledoc """
  RPC sender handling local RPC invocations.
  """

  def replicate(_pid, _partition, id, message, timeout \\ 5_000) do
    case Partition.replicate_message(Partition, id, message, timeout) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, other}
    end
  end

  def tombstone(_pid, _partition, id, timeout \\ 5_000) do
    case PartitionConsumer.accept(PartitionConsumer, id, timeout) do
      :ok -> :ok
      :noop -> :ok
      other -> {:error, other}
    end
  end
end
