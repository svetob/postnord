defmodule Postnord.Rest.RPC do
  alias Postnord.Consumer.PartitionConsumer
  alias Postnord.RPC.Coordinator
  alias Postnord.Id
  alias Postnord.Partition

  def replicate(_queue, id, timestamp, body) do
    {:ok, id} = Id.message_id_decode(id)
    {timestamp, _} = Integer.parse(timestamp)

    case Partition.replicate_message(Partition, id, timestamp, body) do
      :ok ->
        {:ok, 201, "OK"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def tombstone(_queue, id) do
    {:ok, id} = Id.message_id_decode(id)
    case PartitionConsumer.accept(PartitionConsumer, id) do
      :ok ->
        {:ok, 202, "OK"}

      :noop ->
        {:ok, 202, "OK"}

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, other}
    end
  end

  def flush(_queue) do
    case PartitionConsumer.flush(PartitionConsumer) do
      :ok ->
        {:ok, 202, "OK"}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
