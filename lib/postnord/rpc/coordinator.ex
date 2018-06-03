defmodule Postnord.RPC.Coordinator do
  alias Postnord.Cluster.State, as: ClusterState
  alias Postnord.Consumer.Partition
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

    clients_info =
      Enum.map(cluster_state.hosts, fn host ->
        rpc_client_info(host, my_id)
      end)

    children =
      cluster_state.hosts
      |> Enum.map(fn host -> rpc_client_worker(host, my_id) end)
      |> Enum.filter(fn child -> child != nil end)

    {:ok, _pid} = Supervisor.start_link(children, strategy: :one_for_one)

    {:ok, clients_info}
  end

  defp rpc_client_info({host_id, _host_path}, my_id) when host_id == my_id do
    {Postnord.RPC.Client.Local, rpc_client_name("local")}
  end

  defp rpc_client_info({host_id, _host_path}, _my_id) do
    {Postnord.RPC.Client.Rest, rpc_client_name(host_id)}
  end

  defp rpc_client_worker({host_id, _host_path}, my_id) when host_id == my_id do
    nil
  end

  defp rpc_client_worker({host_id, host_path}, _my_id) do
    name = rpc_client_name(host_id)

    %{
      id: name,
      start: {Postnord.RPC.Client.Rest, :start_link, [host_path, [name: name]]}
    }
  end

  defp rpc_client_name(host_id) do
    String.to_atom("rpc_sender_#{host_id}")
  end

  @spec write_message(String.t(), integer) :: :ok | {:error, term}
  def write_message(queue, message, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:write_message, queue, message}, timeout)
  end

  @spec read_message(String.t(), integer) :: {:ok, iolist(), iolist()} | :empty | {:error, term}
  def read_message(queue, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:read_message, queue}, timeout)
  end

  @spec confirm_accept(String.t(), iolist, integer) :: :ok | {:error, term}
  def confirm_accept(queue, id, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:confirm_accept, queue, id}, timeout)
  end

  @spec flush(String.t(), integer) :: :ok | {:error, term}
  def flush(queue, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:flush, queue}, timeout)
  end

  def handle_call({:write_message, _queue, message}, from, hosts) do
    id = Postnord.Id.message_id()
    # TODO Choose partition for queue
    partition = nil
    timestamp = Postnord.now(:nanosecond)

    spawn_link(fn ->
      Logger.debug(fn -> "#{__MODULE__} Coordinating write_message request" end)

      hosts
      |> Enum.map(fn {module, name} ->
        Task.async(module, :replicate, [name, partition, id, timestamp, message])
      end)
      |> handle_response(from)
    end)

    {:noreply, hosts}
  end

  def handle_call({:read_message, queue}, from, hosts) do
    Logger.debug(fn -> "#{__MODULE__} Coordinating read_message request" end)

    spawn_link(fn ->
      response = attempt_read(queue, hosts)
      GenServer.reply(from, response)
    end)

    {:noreply, hosts}
  end

  def handle_call({:confirm_accept, _queue, id}, from, hosts) do
    Logger.debug(fn -> "#{__MODULE__} Coordinating accept request" end)
    # TODO Extract partition from public ID
    partition = nil

    spawn_link(fn ->
      hosts
      |> Enum.map(fn {module, name} ->
        Task.async(module, :tombstone, [name, partition, id])
      end)
      |> handle_response(from)
    end)

    {:noreply, hosts}
  end

  def handle_call({:flush, queue}, from, hosts) do
    Logger.debug(fn -> "#{__MODULE__} Coordinating flush request" end)

    spawn_link(fn ->
      hosts
      |> Enum.map(fn {module, name} ->
        Task.async(module, :flush, [name, queue])
      end)
      |> handle_response(from)
    end)

    {:noreply, hosts}
  end

  defp attempt_read(queue, hosts) do
    case Partition.read(Partition) do
      {:ok, id, bytes} ->
        remote_hosts =
          Enum.filter(hosts, fn {module, _name} ->
            module != Postnord.RPC.Client.Local
          end)

        response =
          remote_hosts
          |> Enum.map(fn {module, name} ->
            Task.async(module, :hold, [name, queue, id])
          end)
          |> handle_hold_response()

        case response do
          :ok ->
            {:ok, id, bytes}

          :reject ->
            attempt_read(queue, hosts)

          :tombstone ->
            :tombstone

          {:error, reason} ->
            {:error, reason}
        end

      :empty ->
        :empty

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_response(tasks, from) do
    count = Enum.count(tasks)

    result =
      count
      |> quorum_required()
      |> await_result_quorum(count)

    GenServer.reply(from, result)
  end

  defp handle_hold_response(tasks) do
    count = Enum.count(tasks)
    required = quorum_required(count + 1)

    await_hold_quorum(tasks, required)
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

  defp await_result_quorum(required, total, _timeout, ok, failed)
       when failed > total - required do
    Logger.warn("Quorum failed: #{required} required, #{ok} succeeded, #{failed} failed")
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
        Logger.warn("Quorum failed: #{required} required, #{ok} responded")
        {:error, "Quorum failed: #{required} required, #{ok} responded"}
    end
  end

  # Await task results and determine if quorum was met
  defp await_hold_quorum(required, total, timeout \\ 5_000, ok \\ 0, failed \\ 0)

  defp await_hold_quorum(required, _total, _timeout, ok, _failed) when ok >= required do
    :ok
  end

  defp await_hold_quorum(required, total, _timeout, ok, failed)
       when failed > total - required do
    Logger.warn("Quorum failed: #{required} required, #{ok} succeeded, #{failed} failed")
    {:error, "Quorum failed: #{required} required, #{ok} succeeded, #{failed} failed"}
  end

  defp await_hold_quorum(required, total, timeout, ok, failed) do
    receive do
      {_ref, :hold} ->
        await_result_quorum(required, total, timeout, ok + 1, failed)

      {_ref, :reject} ->
        await_result_quorum(required, total, timeout, ok, failed + 1)

      {_ref, :tombstone} ->
        :tombstone

      {_ref, {:error, _reason}} ->
        await_result_quorum(required, total, timeout, ok, failed + 1)
    after
      timeout ->
        Logger.warn("Quorum failed: #{required} required, #{ok} responded")
        {:error, "Quorum failed: #{required} required, #{ok} responded"}
    end
  end
end
