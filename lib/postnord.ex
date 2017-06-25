defmodule Postnord do
  import Supervisor.Spec, warn: false
  require Logger

  @moduledoc """
  Postnord main class and launcher.
  """

  def main(args \\ []) do
    :ok = parse_input(args)

    my_id = RandomBytes.uuid()
    Postnord.Cluster.State.start_link(my_id)

    # Start and supervise application processes
    children = worker_postnord() ++
               worker_coordinator() ++
               worker_http_server() ++
               worker_grpc_server()

    Supervisor.start_link(children, [
      strategy: :one_for_one,
      name: Postnord.Supervisor
    ])
  end

  defp worker_postnord do
    data_path = Application.get_env(:postnord, :data_path)
    [worker(Postnord.Partition, [data_path, [name: Postnord.Partition]])]
  end

  defp worker_coordinator do
    [worker(Postnord.RPC.Coordinator, [[], [name: Postnord.RPC.Coordinator]])]
  end

  defp worker_http_server do
    port = Application.get_env(:postnord, :port)
    disabled = Application.get_env(:postnord, :disable_http_server, false)
    if disabled do
      []
    else
      [Plug.Adapters.Cowboy.child_spec(:http, Postnord.Rest, [], [port: port])]
    end
  end

  defp worker_grpc_server do
    port = Application.get_env(:postnord, :grpc_port)
    disabled = Application.get_env(:postnord, :disable_grpc_server, false)
    if disabled do
      []
    else
      [supervisor(GRPC.Server.Supervisor, [{Postnord.GRPC.Node.Server, port}])]
    end
  end

  @doc """
  Create CLI options parser with all configurable options and their descriptions.
  """
  def commando do
    cli = Commando.create("postnord", "an eventually consistent message broker", "mix run -p 2010")

    cli
    |> Commando.with_help()
    |> Commando.with_switch(:port, :integer, "HTTP server port", alias: :p,
                            default: Application.get_env(:postnord, :port))
    |> Commando.with_switch(:grpc_port, :integer, "GRPC server port", alias: :gp,
                            default: Application.get_env(:postnord, :grpc_port))
    |> Commando.with_switch(:disable_http_server, :boolean, "Do not start the HTTP server")
    |> Commando.with_switch(:disable_grpc_server, :boolean, "Do not start the gRPC server")
    |> Commando.with_switch(:data_path, :string, "Data path", alias: :d,
                            default: Application.get_env(:postnord, :data_path))
    |> Commando.with_switch(:replica_nodes, :string, "Semicolon-separated list of replica node URLs", alias: :r,
                            default: :postnord |> Application.get_env(:replica_nodes) |> Enum.join(";"))
    |> Commando.with_switch(:data_path, :string, "Data path", alias: :d,
                            default: Application.get_env(:postnord, :data_path))
  end

  def parse_input(args) do
    cli = commando()

    case Commando.parse(cli, args) do
      {:ok, opts} ->
        opts |> Enum.each(fn {opt, val} -> apply_option(opt, val) end)
        :ok
      {:help, message} ->
        IO.puts message
        System.halt(1)
      {:error, reason} ->
        IO.puts reason
        IO.puts Commando.help_message(cli)
        System.halt(1)
    end
  end

  defp apply_option(:replica_nodes, val) do
    Application.put_env(:postnord, :replica_nodes, val |> String.split(";"))
  end
  defp apply_option(opt, val) do
    Application.put_env(:postnord, opt, val)
  end

  def now(unit \\ :millisecond) do
    :erlang.system_time(unit)
  end
end
