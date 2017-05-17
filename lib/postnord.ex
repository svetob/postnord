defmodule Postnord do
  require Logger

  @moduledoc """
  Postnord main class and launcher.
  """

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    data_path = Application.get_env(:postnord, :data_path)

    children = [
      worker(Postnord.Partition, [data_path, [name: Postnord.Partition]]),
      Plug.Adapters.Cowboy.child_spec(:http, Postnord.Rest, [], [port: 2010])
    ]

    opts = [strategy: :one_for_one, name: Postnord.Supervisor]

    Supervisor.start_link(children, opts)
  end

  def now(unit \\ :millisecond) do
    :erlang.system_time(unit)
  end
end
