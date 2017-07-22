defmodule Postnord.Test.IndexLog do
  use ExUnit.Case, async: false

  require Logger
  alias Postnord.IndexLog
  alias Postnord.IndexLog.Entry
  alias Postnord.IndexLog.State

  @flush_timeout 10
  @flush_timeout_us @flush_timeout * 1000
  @buffer_size 1024

  @path :postnord |> Application.get_env(:data_path) |> Path.join("unit")
  @path_index_log @path |> Path.join("index.log")

  setup do
    state = %State{path: @path_index_log,
                   buffer_size: @buffer_size,
                   flush_timeout: @flush_timeout}

    {:ok, index_log} = IndexLog.start_link(state)
    IO.puts @path_index_log
    {:ok, file} = File.open(@path_index_log)

    on_exit fn ->
      # Remove output files after each test
      File.rm(@path_index_log)
      File.rmdir(@path)
    end

    [pid: index_log, output_file: file]
  end

  test "Writes entry to file", context do
    entry = %Entry{id: "foo", offset: 10, len: 20}

    :ok = IndexLog.write(context[:pid], entry)

    assert log_content(context) == Entry.as_bytes(entry)
  end

  defp log_content(context) do
    IO.read(context[:output_file], :all)
  end

end