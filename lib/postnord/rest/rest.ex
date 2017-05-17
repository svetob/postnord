defmodule Postnord.Rest do
  alias Postnord.Consumer.PartitionConsumer
  alias Postnord.Partition
  use Plug.Router
  use Plug.Builder
  require Logger

  @moduledoc """
  Schuppen REST API endpoints
  """

  plug Plug.Logger
  plug :match
  plug :dispatch

  get "/" do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, "Hello!")
    |> halt
  end

  get "/message/" do
    response_get(conn, get_message())
  end

  defp get_message do
    case PartitionConsumer.read(PartitionConsumer) do
      {:ok, id, message} ->
        if accept(id) do
          {:ok, message}
        else
          get_message()
        end
      other -> other
    end
  end
  defp accept(id) do
    PartitionConsumer.accept(PartitionConsumer, id) == :ok
  end

  defp response_get(conn, {:ok, message}) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, message)
    |> halt
  end
  defp response_get(conn, :empty) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(204, "No message available")
    |> halt
  end
  defp response_get(conn, {:error, reason}) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(500, reason)
    |> halt
  end

  post "/message/" do
    {:ok, body, c} = Plug.Conn.read_body(conn)
    resp = Partition.write_message(Partition, body)
    response_post(c, resp)
  end



  defp response_post(conn, :ok) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(201, "Created")
    |> halt
  end
  defp response_post(conn, {:error, reason}) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(500, reason)
    |> halt
  end
end
