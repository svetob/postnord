defmodule Postnord do
  import Supervisor.Spec, warn: false
  require Logger

  @moduledoc """
  Postnord main class and launcher.
  """

  def main(args \\ []) do
    :ok = parse_input(args)

    data_path = Application.get_env(:postnord, :data_path)
    port = Application.get_env(:postnord, :port)

    children = [
      worker(Postnord.Partition, [data_path, [name: Postnord.Partition]]),
      Plug.Adapters.Cowboy.child_spec(:http, Postnord.Rest, [], [port: port])
    ]

    Supervisor.start_link(children, [
      strategy: :one_for_one,
      name: Postnord.Supervisor
    ])
  end

  def commando do
    Commando.create("postnord", "an eventually consistent message broker", "mix run -p 2010")
    |> Commando.with_help()
    |> Commando.with_switch(:port, :integer, "HTTP server port", alias: :p,
                             default: Application.get_env(:postnord, :port))
    |> Commando.with_switch(:data_path, :string, "Data path", alias: :d,
                             default: Application.get_env(:postnord, :data_path))
    |> Commando.with_switch(:replica_nodes, :string, "Semicolon-separated list of replica node URLs", alias: :r,
                             default: :postnord |> Application.get_env(:replica_nodes) |> Enum.join(";"))
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
