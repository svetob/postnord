defmodule Mix.Tasks.Postnord.Perftest.Read do
  require Logger
  use Mix.Task

  @shortdoc "Index performance test"

  @moduledoc """
  Runs a Postnord indexing test, measuring throughput in DPS and MB/s

  ## Examples

      mix postnord.indextest
      mix postnord.indextest --readers 100 --entries 1000

  ## Command line options

    * `-e`, `--entries` - number of entries to write (default: 100)
    * `-r`, `--readers` - number of concurrent reader processes (default: 10)
  """

  def run(args) do
    {opts, _, _} = OptionParser.parse args,
        switches: [msgbytes: :integer, writers: :integer, entries: :integer, readers: :integer],
        aliases: [m: :msgbytes, w: :writers, e: :entries, r: :readers]

    launch()

    read_test(
        opts[:readers] || 100,
        opts[:entries] || 10)
  end

  defp launch() do
    Postnord.start(nil, nil)
  end

  defp read_test(readers, entries) do
    entries_each = div(entries, readers)
    me = self()

    Logger.info "Read test: #{readers} readers reading #{entries_each} entries each"
    start = Postnord.now()

    1..readers
    |> Enum.map(fn _ ->
      spawn fn ->
        1..entries_each |> Enum.each(fn _ ->
          {:ok, _} = Postnord.Reader.Partition.read(Postnord.Reader.Partition)
        end)
        send me, :ok
      end
    end)
    |> Enum.each(fn _ ->
      receive do
        :ok -> :ok
      end
    end)

    read_entries = entries_each * readers
    read_time_s = (Postnord.now() - start) / 1000
    Logger.info("Read " <>
        "#{Integer.to_string(read_entries)} entries in " <>
        "#{Float.to_string(read_time_s)}s at " <>
        "#{Float.to_string(read_entries / read_time_s)}dps")
  end
end
