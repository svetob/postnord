defmodule Mix.Tasks.Postnord.Perftest.Write do
  require Logger
  import Postnord.Perftest
  use Mix.Task

  @shortdoc "Index performance test"

  @moduledoc """
  Runs a Postnord indexing test, measuring throughput in DPS and MB/s

  ## Examples

      mix postnord.indextest
      mix postnord.indextest --msgbytes 4096 --writers 10 --entries 1000

  ## Command line options

    * `-m`, `--msgbytes` - message size in bytes (default: 102400)
    * `-e`, `--entries` - number of entries to write (default: 10000)
    * `-w`, `--writers` - number of concurrent writer processes (default: 100)
  """

  def run(args) do
    {opts, _, _} = OptionParser.parse args,
        switches: [msgbytes: :integer, writers: :integer, entries: :integer],
        aliases: [m: :msgbytes, w: :writers, e: :entries]

    launch()

    write_test(
        opts[:msgbytes] || 100 * 1024,
        opts[:writers] || 100,
        opts[:entries] || 10000)
  end
end
