defmodule Postnord do
  @moduledoc """
  Postnord main class and launcher.
  """

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      worker(Postnord.Partition, ["postnord.log", [name: Postnord.Partition]])
    ]

    opts = [
      strategy: :one_for_one,
      name: Postnord.Supervisor
    ]

    Supervisor.start_link(children, opts)
  end

  def now(unit \\ :millisecond) do
    :erlang.system_time(unit)
  end

  def index_test() do
    test_start = now()
    me = self()
    1..200 |> Enum.map(fn x ->
        spawn fn ->
          start = now()
          Enum.each(1..10000, fn x ->
            Postnord.Partition.write_message(Postnord.Partition, self(), "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n")
          end)
          IO.inspect "Wrote 10k lines in #{inspect now() - start}ms"
          send me, x
        end
        x
      end)
      |> Enum.each(fn x ->
          receive do
            x -> x
          end
        end)
    IO.inspect "Wrote 500k lines in #{inspect now() - test_start}ms"
  end
end
