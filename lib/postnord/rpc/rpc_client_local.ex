defmodule Postnord.RPC.Client.Local do
  @behaviour Postnord.RPC.Client

  alias Postnord.Partition
  alias Postnord.Consumer

  @moduledoc """
  RPC client handling local RPC invocations.
  """

  def replicate(_pid, _partition, id, timestamp, message, timeout \\ 5_000) do
    Partition.replicate_message(Partition, id, timestamp, message, timeout)
  end

  def hold(_pid, _partition, id, timeout \\ 5_000) do
    Consumer.Partition.hold(Consumer.Partition, id, timeout)
  end

  def tombstone(_pid, _partition, id, timeout \\ 5_000) do
    case Consumer.Partition.accept(Consumer.Partition, id, timeout) do
      :ok -> :ok
      :noop -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, other}
    end
  end

  def flush(_pid, _queue, timeout \\ 5_000) do
    Consumer.Partition.flush(Consumer.Partition, timeout)
  end
end
