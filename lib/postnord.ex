defmodule Postnord do
  require Logger

  @moduledoc """
  Postnord main class and launcher.
  """

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    data_path = Application.get_env(:postnord, :data_path)

    children = [
      worker(Postnord.Partition, [data_path, [name: Postnord.Partition]])
    ]

    opts = [strategy: :one_for_one, name: Postnord.Supervisor]

    Supervisor.start_link(children, opts)
  end

  def now(unit \\ :millisecond) do
    :erlang.system_time(unit)
  end



  def index_test(msg_size \\ 1024, entries \\ 1000, writers \\ 50) do
    msg = :erlang.iolist_to_binary(RandomBytes.base16(Integer.floor_div(msg_size, 2)))

    Logger.info "Index test: #{inspect writers} writers, #{inspect entries} entries at #{Float.to_string(byte_size(msg)/ 1024)}kb each"
    me = self()
    start = now()

    1..writers |> Enum.map(fn x ->
        spawn fn ->
          Enum.each(1..entries, fn _ ->
            Postnord.Partition.write_message(Postnord.Partition, msg)
          end)
          send me, x
        end
        x
      end)
      |> Enum.each(fn x -> receive do x -> :ok end end)

    wrote_mb = (byte_size(msg) * entries * writers) / (1024*1024)
    wrote_entries = entries * writers
    wrote_time_s = (now() - start) / 1000
    Logger.info("Wrote " <>
        "#{Float.to_string(wrote_mb)}Mb, " <>
        "#{Integer.to_string(wrote_entries)} entries in " <>
        "#{Float.to_string(wrote_time_s)}s at " <>
        "#{Float.to_string(wrote_entries / wrote_time_s)}dps, #{Float.to_string(wrote_mb / wrote_time_s)}Mbps")
  end
end
