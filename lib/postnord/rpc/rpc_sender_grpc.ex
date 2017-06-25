defmodule Postnord.RPC.Client.GRPC do
  @behaviour Postnord.RPC.Client

  alias Postnord.GRPC.ReplicateRequest
  alias Postnord.GRPC.ReplicateReply
  alias Postnord.GRPC.TombstoneRequest
  alias Postnord.GRPC.TombstoneReply
  use GenServer

  @moduledoc """
  RPC sender which communicates with other nodes via GRPC.
  """

  @timeout 2_000_000

  def start_link(url, opts \\ []) do
    GenServer.start_link(__MODULE__, url, opts)
  end

  def init(url) do
    {:ok, channel} = GRPC.Stub.connect(url)
    {:ok, channel}
  end

  def replicate(pid, partition, id, message, timeout \\ 5_000) do
    GenServer.call(pid, {:replicate, partition, id, message}, timeout)
  end

  def tombstone(pid, partition, id, timeout \\ 5_000) do
    GenServer.call(pid, {:tombstone, partition, id}, timeout)
  end

  def handle_call({:replicate, partition, id, message}, from, channel) do
    spawn_link fn ->
      request = ReplicateRequest.new(partition: partition, id: id, message: message)
      reply = case Postnord.GRPC.Node.Stub.replicate(channel, request, @timeout) do
        %ReplicateReply{success: true} -> :ok
        %ReplicateReply{error_message: reason} -> {:error, reason}
        other -> {:error, other}
      end
      GenServer.reply(from, reply)
    end
    {:noreply, channel}
  end

  def handle_call({:tombstone, partition, id}, from, channel) do
    spawn_link fn ->
      request = TombstoneRequest.new(partition: partition, id: id)
      reply = case Postnord.GRPC.Node.Stub.tombstone(channel, request, @timeout) do
        %TombstoneReply{success: true} -> :ok
        %TombstoneReply{error_message: reason} -> {:error, reason}
        other -> {:error, other}
      end
      GenServer.reply(from, reply)
    end
    {:noreply, channel}
  end
end
