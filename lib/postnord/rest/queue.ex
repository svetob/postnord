defmodule Postnord.Rest.Queue do
  alias Postnord.Consumer
  alias Postnord.RPC.Coordinator
  alias Postnord.Id
  alias Postnord.Partition

  @moduledoc """
  REST Queue request handler.
  """

  def message_get(_queue) do
    case Consumer.Partition.read(Consumer.Partition) do
      {:ok, id, message} ->
        {:ok, 200, message, %{"message_id" => Id.message_id_encode(id)}}

      :empty ->
        {:ok, 204, "empty"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def message_post(queue, body) do
    case Coordinator.write_message(queue, body) do
      :ok ->
        {:ok, 201, "OK"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def message_accept(queue, id) do
    {:ok, id} = Id.message_id_decode(id)

    case Coordinator.confirm_accept(queue, id) do
      :ok ->
        {:ok, 202, "OK"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def flush(queue) do
    case Coordinator.flush(queue) do
      :ok ->
        {:ok, 202, "OK"}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
