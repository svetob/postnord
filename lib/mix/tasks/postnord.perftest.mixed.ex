defmodule Mix.Tasks.Postnord.Perftest.Mixed do
  require Logger
  import Postnord.Perftest
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
    * `-r`, `--readers` - number of concurrent reader processes (default: 1)
  """

  def run(args) do
    {opts, _, _} = OptionParser.parse args,
        switches: [msgbytes: :integer, writers: :integer, entries: :integer, readers: :integer],
        aliases: [m: :msgbytes, w: :writers, e: :entries, r: :readers]

    launch()

    me = self()
    spawn_link fn ->
      write_test(
          opts[:msgbytes] || 100 * 1024,
          opts[:writers] || 100,
          opts[:entries] || 10_000)
      send me, :ok_write
    end

    spawn_link fn ->
      read_test(
          opts[:readers] || 1,
          opts[:entries] || 10_000)
      send me, :ok_read
    end

    receive do :ok_write -> :ok end
    receive do :ok_read  -> :ok end
  end
end
