defmodule Postnord.RPC.Coordinator do
  alias Postnord.Cluster.State, as: ClusterState
  alias Postnord.Consumer.PartitionConsumer
  import Supervisor.Spec
  require Logger
  use GenServer

  @moduledoc """
  Coordinates cluster-wide processing and responses for incoming client requests.
  """

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(_args) do
    cluster_state = ClusterState.get()
    my_id = cluster_state.my_id
    senders = cluster_state.hosts |> Enum.map(fn host -> rpc_sender(host, my_id) end)

    children = senders
    |> Enum.map(fn {_, _, child} -> child end)
    |> Enum.filter(fn child -> child != nil end)
    Supervisor.start_link(children, [strategy: :one_for_one])

    sender_info = senders |> Enum.map(fn {name, module, _} -> {name, module} end)
    {:ok, sender_info}
  end

  defp rpc_sender({host_id, _host_path}, my_id) when host_id == my_id do
    name = rpc_sender_name("local")
    module = Postnord.RPC.Sender.Local
    {name, module, nil}
  end

  defp rpc_sender({host_id, host_path}, my_id) when host_id != my_id do
    name = rpc_sender_name(host_id)
    module = Postnord.RPC.Sender.GRPC
    child = worker(Postnord.RPC.Sender.GRPC, [host_path, [name: name]])
    {module, name, child}
  end

  defp rpc_sender_name(host_id) do
    String.to_atom("rpc_sender_#{host_id}")
  end


  def write_message(queue, message, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:write_message, queue, message}, timeout)
  end

  def read_message(queue, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:read_message, queue}, timeout)
  end

  def confirm_accept(queue, id, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:confirm_accept, queue, id}, timeout)
  end


  def handle_call({:write_message, _queue, message}, from, hosts) do
    id = Postnord.IdGen.id()
    partition = nil # TODO Choose partition for queue

    spawn_link fn ->
      hosts |> Enum.each(fn {name, module} ->
        Task.async(module, :replicate, [name, partition, id, message])
      end)

      result = hosts
      |> quorum_required()
      |> await_result_quorum(Enum.count(hosts))

      GenServer.reply(from, result)
    end
    {:noreply, hosts}
  end

  def handle_call({:read_message, _queue}, from, hosts) do
    spawn_link fn ->
      GenServer.reply(from, PartitionConsumer.read(PartitionConsumer))
    end
    {:noreply, hosts}
  end

  def handle_call({:confirm_accept, _queue, id}, from, hosts) do
    partition = nil # TODO Extract partition from public ID

    spawn_link fn ->
      hosts |> Enum.each(fn {name, module} ->
        Task.async(module, :tombstone, [name, partition, id])
      end)

      result = hosts
      |> quorum_required()
      |> await_result_quorum(Enum.count(hosts))

      GenServer.reply(from, result)
    end
    {:noreply, hosts}
  end

  # Calculate required quorum for host set
  defp quorum_required(hosts) do
    node_count = Enum.count(hosts)
    round(Float.floor(node_count/2)) + 1
  end

  # Await task results and determine if quorum was met
  defp await_result_quorum(required, total, timeout \\ 5_000, ok \\ 0, failed \\ 0)
  defp await_result_quorum(required, _total, _timeout, ok, _failed) when ok >= required do
    :ok
  end
  defp await_result_quorum(required, total, _timeout, ok, failed) when failed > (total - required) do
    {:error, "Quorum failed: #{required} required, #{ok} succeeded, #{failed} failed"}
  end
  defp await_result_quorum(required, total, timeout, ok, failed) do
    receive do
      {_ref, :ok} ->
        await_result_quorum(required, total, timeout, ok + 1, failed)
      {_ref, {:error, _reason}} ->
        await_result_quorum(required, total, timeout, ok, failed + 1)
    after
      timeout ->
        {:error, "Quorum failed: #{required} required, #{ok} responded"}
    end
  end
end
