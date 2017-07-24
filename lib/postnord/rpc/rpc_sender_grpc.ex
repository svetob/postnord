defmodule Postnord.RPC.Client.GRPC do
  @behaviour Postnord.RPC.Client

  alias Postnord.GRPC.FlushRequest
  alias Postnord.GRPC.GenericReply
  alias Postnord.GRPC.ReplicateRequest
  alias Postnord.GRPC.ReplicateReply
  alias Postnord.GRPC.TombstoneRequest
  require Logger
  use GenServer

  @moduledoc """
  RPC sender which communicates with other nodes via GRPC.
  """

  @timeout 2_000_000
  @opts [timeout: @timeout]

  def start_link(url, opts \\ []) do
    GenServer.start_link(__MODULE__, url, opts)
  end

  def init(url) do
    {:ok, {url, nil}}
  end

  defp ensure_channel(nil, url) do
    Logger.debug "#{__MODULE__} Connecting to #{url}"
    {:ok, channel} = GRPC.Stub.connect(url)
    Logger.debug "#{__MODULE__} Connected to #{url}"
    channel
  end
  defp ensure_channel(channel, _url) do
    channel
  end

  def replicate(pid, partition, id, timestamp, message, timeout \\ 5_000) do
    GenServer.call(pid, {:replicate, partition, id, timestamp, message}, timeout)
  end

  def tombstone(pid, partition, id, timeout \\ 5_000) do
    GenServer.call(pid, {:tombstone, partition, id}, timeout)
  end

  def flush(pid, queue, timeout \\ 5_000) do
    GenServer.call(pid, {:flush, queue}, timeout)
  end

  def handle_call({:replicate, partition, id, timestamp, message}, from, {url, channel}) do
    channel = ensure_channel(channel, url)
    spawn_link fn ->
      Logger.debug "#{__MODULE__} Sending Replicate request to #{url}"
      request = ReplicateRequest.new(partition: partition, id: id, timestamp: timestamp, message: message)

      channel
      |> Postnord.GRPC.Node.Stub.replicate(request, timeout: 1_000_000)
      |> handle_reply(from)
    end
    {:noreply, {url, channel}}
  end

  def handle_call({:flush, queue}, from, {url, channel}) do
      channel = ensure_channel(channel, url)
      spawn_link fn ->
        Logger.debug "#{__MODULE__} Sending ReplicateFlush request to #{url}"
        request = FlushRequest.new(queue: queue)

        channel
        |> Postnord.GRPC.Node.Stub.replicate_flush(request, timeout: 1_000_000)
        |> handle_reply(from)
      end
      {:noreply, {url, channel}}
    end

  def handle_call({:tombstone, partition, id}, from, {url, channel}) do
    channel = ensure_channel(channel, url)
    spawn_link fn ->
      Logger.debug "#{__MODULE__} Sending Tombstone request to #{url}"
      request = TombstoneRequest.new(partition: partition, id: id)

      channel
      |> Postnord.GRPC.Node.Stub.tombstone(request, timeout: 1_000_000)
      |> handle_reply(from)
    end
    {:noreply, {url, channel}}
  end

  defp handle_reply(%GenericReply{success: true}, from) do
    GenServer.reply(from, :ok)
  end
  defp handle_reply(%GenericReply{error_message: reason}, from) do
    GenServer.reply(from, {:error, reason})
  end
end
