defmodule Postnord.Rest.Router do
  use Plug.Router

  alias Postnord.Rest.{Status, Queue, RPC}

  plug :match
  plug :dispatch

  get "_status" do
    resp = Status.status()
    respond(resp, conn)
  end

  get "queue/:queue/message" do
    conn.params["queue"]
    |> Queue.message_get()
    |> respond(conn)
  end

  post "queue/:queue/message" do
    case read_full_body(conn) do
      {:ok, body, conn} ->
        conn.params["queue"]
        |> Queue.message_post(body)
        |> respond(conn)

      {:error, reason} ->
        respond({:error, reason}, conn)
    end
  end

  post "queue/:queue/message/:id/accept" do
    conn.params["queue"]
    |> Queue.message_accept(conn.params["id"])
    |> respond(conn)
  end

  post "queue/:queue/flush" do
    conn.params["queue"]
    |> Queue.flush()
    |> respond(conn)
  end

  post "rpc/queue/:queue/message/:id/timestamp/:timestamp/replicate" do
    case read_full_body(conn) do
      {:ok, body, conn} ->
        conn.params["queue"]
        |> RPC.replicate(conn.params["id"], conn.params["timestamp"], body)
        |> respond(conn)

      {:error, reason} ->
        respond({:error, reason}, conn)
    end
  end

  post "rpc/queue/:queue/message/:id/tombstone" do
    conn.params["queue"]
    |> RPC.tombstone(conn.params["id"])
    |> respond(conn)
  end

  post "rpc/queue/:queue/flush" do
    conn.params["queue"]
    |> RPC.flush()
    |> respond(conn)
  end

  match _ do
    send_resp(conn, 405, "")
  end

  defp respond({:ok, status, body}, conn) do
    send_resp(conn, status, body)
  end
  defp respond({:ok, status, body, headers}, conn) do
    conn = Enum.reduce(headers, conn, fn {key, value}, conn ->
      put_resp_header(conn, key, value)
    end)
    send_resp(conn, status, body)
  end
  defp respond({:error, reason}, conn) when is_binary(reason) do
    send_resp(conn, 500, reason)
  end
  defp respond({:error, reason}, conn) do
    send_resp(conn, 500, inspect(reason))
  end

  @spec read_full_body(Plug.Conn.t) :: {:ok, binary, Plug.Conn.t} | {:error, term}
  defp read_full_body(conn, body \\ <<>>) do
    case read_body(conn) do
      {:ok, binary, conn_next} ->
        {:ok, binary, conn_next}

      {:more, binary, conn_next} ->
        read_full_body(conn_next, body <> binary)

      {:error, reason} ->
        {:error, reason}
    end
  end
end
