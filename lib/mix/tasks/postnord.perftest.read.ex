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
    * `-r`, `--readers` - number of concurrent reader processes (default: 1)
  """

  def run(args) do
    {opts, _, _} = OptionParser.parse args,
        switches: [entries: :integer, readers: :integer],
        aliases: [e: :entries, r: :readers]

    launch()

    readers = opts[:readers] || 1
    entries = opts[:entries] || 100

    read_test(readers, entries)
  end
end
