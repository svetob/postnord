defmodule Mix.Tasks.Postnord.Perftest.WriteRead do
  require Logger
  use Mix.Task

  @shortdoc "Index performance test"

  @moduledoc """
  Runs a Postnord indexing test, measuring throughput in DPS and MB/s

  ## Examples

      mix postnord.indextest
      mix postnord.indextest --msgbytes 4096 --writers 10 --readers 100 --entries 1000

  ## Command line options

    * `-m`, `--msgbytes` - message size in bytes (default: 102400)
    * `-e`, `--entries` - number of entries to write (default: 10000)
    * `-w`, `--writers` - number of concurrent writer processes (default: 100)
    * `-r`, `--readers` - number of concurrent reader processes (default: 100)
  """

  def run(args) do
    {opts, _, _} = OptionParser.parse args,
        switches: [msgbytes: :integer, writers: :integer, entries: :integer, readers: :integer],
        aliases: [m: :msgbytes, w: :writers, e: :entries, r: :readers]

    launch()

    write_test(
        opts[:msgbytes] || 100 * 1024,
        opts[:writers] || 100,
        opts[:entries] || 100)

    read_test(
        opts[:readers] || 100,
        opts[:entries] || 100)
  end

  defp launch() do
    Postnord.start(nil, nil)
  end

  defp write_test(msgbytes, writers, entries) do
    msg = :erlang.iolist_to_binary(RandomBytes.base16(Integer.floor_div(msgbytes, 2)))
    entries_each = div(entries, writers)
    me = self()

    Logger.info "Write test: #{writers} writers, #{entries_each} entries each at #{Float.to_string(byte_size(msg)/ 1024)}kb each"
    start = Postnord.now()

    1..writers
    |> Enum.map(fn _ ->
      spawn fn ->
        1..entries_each |> Enum.each(fn _ ->
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

    wrote_entries = entries_each * writers
    wrote_mb = (byte_size(msg) * wrote_entries) / (1024*1024)
    wrote_time_s = (Postnord.now() - start) / 1000
    Logger.info("Wrote " <>
        "#{Float.to_string(wrote_mb)}Mb, " <>
        "#{Integer.to_string(wrote_entries)} entries in " <>
        "#{Float.to_string(wrote_time_s)}s at " <>
        "#{Float.to_string(wrote_entries / wrote_time_s)}dps, #{Float.to_string(wrote_mb / wrote_time_s)}Mbps")
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
