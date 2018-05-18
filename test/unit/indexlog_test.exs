defmodule Postnord.Test.IndexLog do
  use ExUnit.Case, async: false

  require Logger
  alias Postnord.IndexLog
  alias Postnord.IndexLog.Entry
  alias Postnord.IndexLog.State

  @flush_timeout 20
  @flush_timeout_us @flush_timeout * 1000
  @buffer_size 128

  @path :postnord |> Application.get_env(:data_path) |> Path.join("unit")
  @path_index_log @path |> Path.join("index.log")

  setup do
    state = %State{path: @path_index_log,
                   buffer_size: @buffer_size,
                   flush_timeout: @flush_timeout}

    {:ok, index_log} = IndexLog.start_link(state)
    {:ok, file} = File.open(@path_index_log)

    on_exit fn ->
      # Remove output files after each test
      File.rm(@path_index_log)
      File.rmdir(@path)
    end

    [pid: index_log, output_file: file]
  end

  test "Writes entry to file", context do
    entry = random_entry()

    :ok = IndexLog.write(context[:pid], entry)

    assert log_content(context) == Entry.as_bytes(entry)
  end

  test "Buffers writes until flush_timeout", context do
    me = self()
    spawn_link fn ->
      :ok = IndexLog.write(context[:pid], random_entry())
      send me, :write_ok
    end

    refute_receive :write_ok, @flush_timeout - 1
    assert_receive :write_ok, @flush_timeout
  end

  test "Buffers writes until buffer_size", context do
    # Send single message
    {time_a, _} = :timer.tc fn ->
      :ok = IndexLog.write(context[:pid], random_entry())
    end
    # Send single  message
    {time_b, _} = :timer.tc fn ->
      :ok = IndexLog.write(context[:pid], random_entry())
    end
    # Write enough messages to exceed buffer size
    {time_large, _} = :timer.tc fn ->
      1..4 |> Enum.map(fn n ->
        Task.async(fn -> IndexLog.write(context[:pid], random_entry()) end)
      end)
      |> Enum.each(fn t ->
        :ok = Task.await(t)
      end)
    end
    # Send small message
    {time_c, _} = :timer.tc fn ->
      :ok = IndexLog.write(context[:pid], random_entry())
    end

    assert time_a >= @flush_timeout_us
    assert time_b >= @flush_timeout_us
    assert time_c >= @flush_timeout_us
    assert time_large < @flush_timeout_us
  end

  defp log_content(context) do
    IO.binread(context[:output_file], :all)
  end

  def random_entry do
    %Entry{id: Postnord.Id.message_id(), offset: 10, len: 20}
  end

end
