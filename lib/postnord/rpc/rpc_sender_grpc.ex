defmodule Postnord.RPC.Client.GRPC do
  @behaviour Postnord.RPC.Client

  alias Postnord.GRPC.ReplicateRequest
  alias Postnord.GRPC.ReplicateReply
  alias Postnord.GRPC.TombstoneRequest
  alias Postnord.GRPC.TombstoneReply
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
    channel
  end
  defp ensure_channel(channel, _url) do
    channel
  end

  def replicate(pid, partition, id, message, timeout \\ 5_000) do
    GenServer.call(pid, {:replicate, partition, id, message}, timeout)
  end

  def tombstone(pid, partition, id, timeout \\ 5_000) do
    GenServer.call(pid, {:tombstone, partition, id}, timeout)
  end

  def handle_call({:replicate, partition, id, message}, from, {url, channel}) do
    channel = ensure_channel(channel, url)
    spawn_link fn ->
      request = ReplicateRequest.new(partition: partition, id: id, message: message)
      Logger.debug "#{__MODULE__} Sending replicate request to #{url}"
      reply = case Postnord.GRPC.Node.Stub.replicate(channel, request, timeout: 1_000_000) do
        %ReplicateReply{success: true} -> :ok
        %ReplicateReply{error_message: reason} -> {:error, reason}
        other -> {:error, other}
      end
      Logger.debug "#{__MODULE__} Replicate reply received from #{url}"
      GenServer.reply(from, reply)
    end
    {:noreply, {url, channel}}
  end

  def handle_call({:tombstone, partition, id}, from, {url, channel}) do
    channel = ensure_channel(channel, url)
    spawn_link fn ->
      Logger.debug "#{__MODULE__} Sending tombstone request to #{url}"
      request = TombstoneRequest.new(partition: partition, id: id)
      reply = case Postnord.GRPC.Node.Stub.tombstone(channel, request, timeout: 1_000_000) do
        %TombstoneReply{success: true} -> :ok
        %TombstoneReply{error_message: reason} -> {:error, reason}
        other -> {:error, other}
      end
      Logger.debug "#{__MODULE__} Tombstone reply received from #{url}"
      GenServer.reply(from, reply)
    end
    {:noreply, {url, channel}}
  end
end
