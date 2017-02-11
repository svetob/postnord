defmodule Mix.Tasks.Postnord.Indextest do
  require Logger
  use Mix.Task

  @shortdoc "Index performance test"

  @moduledoc """
  Runs a Postnord indexing test, measuring throughput in DPS and MB/s

  ## Examples

      mix postnord.indextest
      mix postnord.indextest --msgbytes 4096 --writers 10 --entries 1000

  ## Command line options

    * `-m`, `--msgbytes` - message size in bytes (default: 102400)
    * `-w`, `--writers` - number of concurrent writer processes (default: 100)
    * `-e`, `--entries` - number of entries to write per writer process (default: 100)
  """

  def run(args) do
    {opts, _, _} = OptionParser.parse args,
        switches: [msgbytes: :integer, writers: :integer, entries: :integer],
        aliases: [m: :msgbytes, w: :writers, e: :entries]

    launch()

    index_test(
        opts[:msgbytes] || 100 * 1024,
        opts[:writers] || 100,
        opts[:entries] || 100)
  end

  defp launch() do
    Postnord.start(nil, nil)
  end

  defp index_test(msgbytes, writers, entries) do
    msg = :erlang.iolist_to_binary(RandomBytes.base16(Integer.floor_div(msgbytes, 2)))

    Logger.info "Index test: #{inspect writers} writers, #{inspect entries} entries at #{Float.to_string(byte_size(msg)/ 1024)}kb each"
    me = self()
    start = Postnord.now()

    1..writers
      |> Enum.map(fn x ->
        spawn fn ->
          Enum.each(1..entries, fn _ ->
            Postnord.Partition.write_message(Postnord.Partition, msg)
          end)
          send me, :ok
        end
      end)
      |> Enum.each(fn _ ->
        receive do
          :ok -> :ok
        end
      end)

    wrote_mb = (byte_size(msg) * entries * writers) / (1024*1024)
    wrote_entries = entries * writers
    wrote_time_s = (Postnord.now() - start) / 1000
    Logger.info("Wrote " <>
        "#{Float.to_string(wrote_mb)}Mb, " <>
        "#{Integer.to_string(wrote_entries)} entries in " <>
        "#{Float.to_string(wrote_time_s)}s at " <>
        "#{Float.to_string(wrote_entries / wrote_time_s)}dps, #{Float.to_string(wrote_mb / wrote_time_s)}Mbps")
  end
end
