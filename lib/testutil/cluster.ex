defmodule TestUtil.Cluster do
  require Logger

  @moduledoc """
  Utilities for creating and managing a multi-node test cluster.

  [ ] The nodes run on localhost and communicate via HTTP
  [ ] Data is stored under /test/data
  [ ] Test data is cleaned up after termination
  """

  @doc """
  Creates a local cluster with one node for each HTTP port given as argument.
  """
  def create(ports \\ [2011, 2012, 2013]) do
    Logger.info "#{__MODULE__} Launching test cluster"
    enable_node_boot()

    nodes = ports
    |> Enum.map(fn port -> {Postnord.Id.node_id(), port} end)

    nodes
    |> Enum.map(&Task.async(fn -> spawn_node(nodes, &1) end))
    |> Enum.map(&Task.await(&1, 10_000))
    |> Enum.map(fn {:ok, node} -> node end)
  end

  @doc """
  Shut down cluster and clean up test data
  """
  def teardown(cluster) do
    cluster |> Enum.each(&:slave.stop/1)
  end

  defp spawn_node(all_nodes, {id, port}) do
    {:ok, node} = :slave.start('127.0.0.1', String.to_atom("node#{port}"), slave_args())
    Logger.debug fn -> "#{__MODULE__} Starting slave #{id} with node name #{inspect node}" end

    add_code_paths(node)
    transfer_configuration(node)
    apply_new_configurations(all_nodes, node, id, port)
    ensure_applications_started(node)
    add_elixir_code_files(node)
    start_postnord(node)

    {:ok, node}
  end

  def slave_args do
    '-loader inet -hosts 127.0.0.1 -setcookie #{:erlang.get_cookie()}'
  end

  # Enable the ability to spawn replicas of this VM
  def enable_node_boot do
    # Turn node into a distributed node with the given long name
    Node.start(:"primary@127.0.0.1")
    Process.sleep(100)

    # Allow spawned nodes to fetch all code from this node
    :erl_boot_server.start([])
    {:ok, ipv4} = :inet.parse_ipv4_address('127.0.0.1')
    :erl_boot_server.add_slave(ipv4)
  end

  defp add_code_paths(node) do
    :rpc.block_call(node, :code, :add_paths, [:code.get_path()])
  end

  # Add all non-test elixir files
  defp add_elixir_code_files(node) do
    Code.loaded_files()
    |> Enum.filter(fn x ->
      String.contains?(to_string(x), "test") == false
    end)
    |> Enum.each(fn file ->
      :rpc.block_call(node, Code, :load_file, [file])
    end)
  end

  defp transfer_configuration(node) do
    for {app_name, _, _} <- Application.loaded_applications do
      for {key, val} <- Application.get_all_env(app_name) do
        :rpc.block_call(node, Application, :put_env, [app_name, key, val])
      end
    end
  end

  defp apply_new_configurations(all_nodes, node, id, http_port) do
    replica_nodes = all_nodes |> Enum.map(fn {id, port} ->
      {id, "localhost:#{port}"}
    end)

    envs = [
      [:postnord, :data_path, "test/data/cluster/#{id}/"],
      [:postnord, :port, http_port],
      [:postnord, :node_id, id],
      [:postnord, :replica_nodes, replica_nodes]
    ]

    envs |> Enum.each(fn env ->
      :rpc.block_call(node, Application, :put_env, env)
    end)
  end

  defp ensure_applications_started(node) do
    :rpc.block_call(node, Application, :ensure_all_started, [:mix])
    :rpc.block_call(node, Mix, :env, [Mix.env()])
    for {app_name, _, _} <- Application.loaded_applications do
      :rpc.block_call(node, Application, :ensure_all_started, [app_name])
    end
  end

  defp start_postnord(node) do
    :ok == :rpc.cast(node, TestUtil.Cluster, :cluster_node_start, [])
  end

  @doc """
  Start a node and prevent process from closing
  """
  def cluster_node_start do
    Postnord.main([])
    keepalive()
  end

  defp keepalive do
    Process.sleep(10_000)
    keepalive()
  end
end
