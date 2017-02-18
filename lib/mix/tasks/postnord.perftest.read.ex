defmodule Mix.Tasks.Postnord.Perftest.Read do
  require Logger
  import Postnord.Perftest
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
end
