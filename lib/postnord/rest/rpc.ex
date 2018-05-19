defmodule Postnord.Rest.RPC do
  alias Postnord.Consumer
  alias Postnord.RPC.Coordinator
  alias Postnord.Id
  alias Postnord.Partition

  @moduledoc """
  REST RPC request handler.
  """

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

    case Consumer.Partition.accept(Consumer.Partition, id) do
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
    case Consumer.Partition.flush(Consumer.Partition) do
      :ok ->
        {:ok, 202, "OK"}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
