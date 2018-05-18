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

    sender_info = senders |> Enum.map(fn {module, name, _} -> {module, name} end)
    {:ok, sender_info}
  end

  defp rpc_sender({host_id, _host_path}, my_id) when host_id == my_id do
    module = Postnord.RPC.Client.Local
    name = rpc_sender_name("local")
    {module, name, nil}
  end

  defp rpc_sender({host_id, host_path}, my_id) when host_id != my_id do
    module = Postnord.RPC.Client.HTTP
    name = rpc_sender_name(host_id)
    child = worker(Postnord.RPC.Client.HTTP, [host_path, [name: name]], [id: name])
    {module, name, child}
  end

  defp rpc_sender_name(host_id) do
    String.to_atom("rpc_sender_#{host_id}")
  end

  @spec write_message(String.t, integer) :: :ok | {:error, term}
  def write_message(queue, message, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:write_message, queue, message}, timeout)
  end

  @spec read_message(String.t, integer) :: :ok | {:error, term}
  def read_message(queue, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:read_message, queue}, timeout)
  end

  @spec confirm_accept(String.t, iolist, integer) :: :ok | {:error, term}
  def confirm_accept(queue, id, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:confirm_accept, queue, id}, timeout)
  end

  @spec flush(String.t, integer) :: :ok | {:error, term}
  def flush(queue, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:flush, queue}, timeout)
  end


  def handle_call({:write_message, _queue, message}, from, hosts) do
    id = Postnord.Id.message_id()
    partition = nil # TODO Choose partition for queue
    timestamp = Postnord.now(:nanosecond)

    spawn_link fn ->
      Logger.debug "#{__MODULE__} Coordinating write_message request"
      hosts
      |> Enum.map(fn {module, name} ->
        Task.async(module, :replicate, [name, partition, id, timestamp, message])
      end)
      |> handle_response(from)
    end
    {:noreply, hosts}
  end

  def handle_call({:read_message, _queue}, from, hosts) do
    Logger.debug "#{__MODULE__} Coordinating read_message request"
    spawn_link fn ->
      GenServer.reply(from, PartitionConsumer.read(PartitionConsumer))
    end
    {:noreply, hosts}
  end

  def handle_call({:confirm_accept, _queue, id}, from, hosts) do
    Logger.debug "#{__MODULE__} Coordinating accept request"
    partition = nil # TODO Extract partition from public ID

    spawn_link fn ->
      hosts
      |> Enum.map(fn {module, name} ->
        Task.async(module, :tombstone, [name, partition, id])
      end)
      |> handle_response(from)
    end
    {:noreply, hosts}
  end

  def handle_call({:flush, queue}, from, hosts) do
    Logger.debug "#{__MODULE__} Coordinating flush request"
    spawn_link fn ->
      hosts
      |> Enum.map(fn {module, name} ->
        Task.async(module, :flush, [name, queue])
      end)
      |> handle_response(from)
    end
    {:noreply, hosts}
  end


  defp handle_response(tasks, from) do
    count = Enum.count(tasks)

    result = count
    |> quorum_required()
    |> await_result_quorum(count)

    GenServer.reply(from, result)
  end

  # Calculate required quorum for host set
  defp quorum_required(node_count) do
    round(Float.floor(node_count / 2)) + 1
  end

  # Await task results and determine if quorum was met
  defp await_result_quorum(required, total, timeout \\ 5_000, ok \\ 0, failed \\ 0)
  defp await_result_quorum(required, _total, _timeout, ok, _failed) when ok >= required do
    :ok
  end
  defp await_result_quorum(required, total, _timeout, ok, failed) when failed > (total - required) do
    Logger.warn "Quorum failed: #{required} required, #{ok} succeeded, #{failed} failed"
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
        Logger.warn "Quorum failed: #{required} required, #{ok} responded"
        {:error, "Quorum failed: #{required} required, #{ok} responded"}
    end
  end
end
