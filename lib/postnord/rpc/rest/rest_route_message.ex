defmodule Postnord.Rest.Route.Message do
  alias Postnord.Consumer.PartitionConsumer
  alias Postnord.RPC.Coordinator

  @moduledoc """
  GET and POST message for any queue.
  """

  def init(req, state) do
    queue = :cowboy_req.binding(:queue, req)
    {:ok, reply} = handle(req, queue)
    {:ok, reply, state}
  end

  def handle(%{method: "GET"} = req, queue) do
    queue |> queue_read() |> reply_get(req)
  end
  def handle(%{method: "POST"} = req, queue) do
    case read_full_request_body(req) do
      {:ok, body, req_next} ->
          queue |> queue_write(body) |> reply_post(req_next)
      {:error, reason} ->
          reply_post({:error, reason}, req)
    end
  end
  def handle(req, _queue) do
    :cowboy_req.reply(405, req)
  end

  defp reply_get({:ok, message}, req), do:
    :cowboy_req.reply(200, %{"content-type" => "text/plain"}, message, req)
  defp reply_get(:empty, req), do:
    :cowboy_req.reply(204, req)
  defp reply_get({:error, reason}, req), do:
    :cowboy_req.reply(500, [{"content-type", "text/plain"}], reason, req)

  defp reply_post(:ok, req), do:
    :cowboy_req.reply(201, req)
  defp reply_post({:error, reason}, req), do:
    :cowboy_req.reply(500, [{"content-type", "text/plain"}], reason, req)

  defp read_full_request_body(req) do
    case :cowboy_req.read_body(req) do
      {:ok, body, req2} ->
        {:ok, body, req2}
      {:more, body, req2} ->
        case read_full_request_body(req2) do
          {:ok, body_next, req_next} -> {:ok, body <> body_next, req_next}
          {:error, reason} -> {:error, reason}
        end
      {:error, reason} -> {:error, reason}
    end
  end

  defp queue_read(queue) do
    case PartitionConsumer.read(PartitionConsumer) do
      {:ok, id, message} ->
        if accept(id) do
          {:ok, message}
        else
          queue_read(queue)
        end
      other ->
        other
    end
  end

  defp queue_write(queue, message) do
    Coordinator.write_message(queue, message)
  end

  defp accept(id) do
    PartitionConsumer.accept(PartitionConsumer, id) == :ok
  end
end
