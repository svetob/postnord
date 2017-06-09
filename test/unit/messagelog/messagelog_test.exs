defmodule Postnord.Test.MessageLog do
  use ExUnit.Case, async: false

  require Logger
  alias Postnord.MessageLog
  alias Postnord.MessageLog.State

  @moduledoc """
  Unit tests for message log.

  The tests ask the message log to write messages to disk, verifies the
  responses, the timing of disk writes, and the contents of the written files.
  """

  @flush_timeout 10
  @flush_timeout_us @flush_timeout * 1000
  @buffer_size 1024


  setup do
    path = Application.get_env(:postnord, :test_data_path)
    file_path = Path.join(path, "message.log")
    state = %State{path: path,
                   flush_timeout: @flush_timeout,
                   buffer_size: @buffer_size}

    {:ok, message_log} = MessageLog.start_link(state)
    {:ok, file} = File.open(file_path, [:binary, :read])

    on_exit fn ->
      # Remove output files after each test
      File.rm(file_path)
      File.rmdir(path)
    end

    [pid: message_log, output_file: file]
  end


  test "Writes message to file", context do
    msg = "Hello World!"

    {:ok, _, _} = MessageLog.write(context[:pid], msg)

    assert log_content(context) == msg
  end


  test "Returns correct offset, len of writes", context do
    {:ok, 0, 12} = MessageLog.write(context[:pid], "Hello world!")
    {:ok, 12, 10} = MessageLog.write(context[:pid], "Nice suit!")
    {:ok, 22, 15} = MessageLog.write(context[:pid], "Why, thank you!")
  end


  test "Writes messages in order", context do
    msgs = 1..5 |> Enum.map(fn _ -> RandomBytes.base16(10) end)

    msgs |> Enum.each(fn msg ->
      {:ok, _, _} = MessageLog.write(context[:pid], msg)
    end)

    assert IO.read(context[:output_file], :all) == Enum.join(msgs)
  end

  test "Sends response to correct process", context do
    sample_size = 100
    me = self()

    1..sample_size |> Enum.each(fn n ->
      spawn_link fn ->
        msg = RandomBytes.base16(n)
        {:ok, _, _} = MessageLog.write(context[:pid], msg)
        send me, {:ok, n}
      end
    end)

    1..sample_size |> Enum.each(fn n ->
      assert_receive {:ok, ^n}, 2000
    end)
  end


  test "Buffers writes until flush_timeout", context do
    me = self()
    spawn_link fn ->
      {:ok, _, _} = MessageLog.write(context[:pid], "Foo")
      send me, :write_ok
    end

    refute_receive :write_ok, @flush_timeout - 1
    assert_receive :write_ok, 5
  end


  test "Buffers writes until buffer_size", context do
    # Send small message
    {time_a, _} = :timer.tc fn ->
      {:ok, _, _} = MessageLog.write(context[:pid], "A")
    end
    # Send small message
    {time_b, _} = :timer.tc fn ->
      {:ok, _, _} = MessageLog.write(context[:pid], "B")
    end
    # Send message of size buffer_size
    IO.puts "Big write"
    msg_big = Enum.join(List.duplicate("X", @buffer_size))
    {time_large, _} = :timer.tc fn ->
      {:ok, _, _} = MessageLog.write(context[:pid], msg_big)
    end
    # Send small message
    {time_c, _} = :timer.tc fn ->
      {:ok, _, _} = MessageLog.write(context[:pid], "C")
    end

    assert time_a >= @flush_timeout_us
    assert time_b >= @flush_timeout_us
    assert time_c >= @flush_timeout_us
    assert time_large < @flush_timeout_us
  end


  defp log_content(context) do
     IO.read(context[:output_file], :all)
  end
end
