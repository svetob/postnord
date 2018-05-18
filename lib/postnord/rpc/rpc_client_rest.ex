defmodule Postnord.RPC.Client.Rest do
  use GenServer

  require Logger

  alias Postnord.Id

  @behaviour Postnord.RPC.Client

  @http_headers [{"Content-Type", "text/plain"}]
  @http_options [hackney: [pool: :default]]

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(node_uri) do
    {:ok, node_uri}
  end

  def replicate(pid, partition, id, timestamp, message, timeout \\ 5_000) do
    GenServer.call(pid, {:replicate, partition, id, timestamp, message}, timeout)
  end

  def tombstone(pid, partition, id, timeout \\ 5_000) do
    GenServer.call(pid, {:tombstone, id}, timeout)
  end

  def flush(pid, queue, timeout \\ 5_000) do
    GenServer.call(pid, {:flush, queue}, timeout)
  end

  def handle_call({:replicate, _partition, id, timestamp, message}, from, node_uri) do
    spawn_link fn ->
        id_encoded = Id.message_id_encode(id)
        uri = node_uri <> "/queue/q/message/#{id_encoded}/timestamp/#{timestamp}/replicate"

        Logger.debug "#{__MODULE__} Sending replicate request to #{uri}"

        case HTTPoison.post(uri, message, @http_headers, @http_options) do
          {:ok, %HTTPoison.Response{status_code: 201}} ->
            GenServer.reply(from, :ok)

          {:ok, %HTTPoison.Response{status_code: status_code}} ->
            GenServer.reply(from, {:error, "Unexpected status code on replication: #{status_code}"})

          {:error, %HTTPoison.Error{reason: reason}} ->
            GenServer.reply(from, {:error, reason})
        end
      end
      {:noreply, node_uri}
  end

  def handle_call({:tombstone, _partition, id}, from, node_uri) do
    spawn_link fn ->
        id_encoded = Id.message_id_encode(id)
        uri = node_uri <> "/queue/q/message/#{id_encoded}/tombstone"

        Logger.debug "#{__MODULE__} Sending tombstone request to #{uri}"

        case HTTPoison.post(uri, "", @http_headers, @http_options) do
          {:ok, %HTTPoison.Response{status_code: 202}} ->
            GenServer.reply(from, :ok)

          {:ok, %HTTPoison.Response{status_code: status_code}} ->
            GenServer.reply(from, {:error, "Unexpected status code on tombstone: #{status_code}"})

          {:error, %HTTPoison.Error{reason: reason}} ->
            GenServer.reply(from, {:error, reason})
        end
      end
      {:noreply, node_uri}
  end

  def handle_call({:flush, queue}, from, node_uri) do
    spawn_link fn ->
        uri = node_uri <> "/queue/#{queue}/flush"

        Logger.debug "#{__MODULE__} Sending flush request to #{uri}"

        case HTTPoison.post(uri, "", @http_headers, @http_options) do
          {:ok, %HTTPoison.Response{status_code: 202}} ->
            GenServer.reply(from, :ok)

          {:ok, %HTTPoison.Response{status_code: status_code}} ->
            GenServer.reply(from, {:error, "Unexpected status code on flush: #{status_code}"})

          {:error, %HTTPoison.Error{reason: reason}} ->
            GenServer.reply(from, {:error, reason})
        end
      end
      {:noreply, node_uri}
  end

end
