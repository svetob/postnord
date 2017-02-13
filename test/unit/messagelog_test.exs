defmodule Postnord.Test.MessageLog do
  use ExUnit.Case, async: false

  require Logger
  alias Postnord.MessageLog
  alias Postnord.MessageLog.State

  @flush_timeout 10
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

    :ok = MessageLog.write(context[:pid], msg, nil)

    assert_receive {:"$gen_cast", {:write_messagelog_ok, _, _, _}}
    assert IO.read(context[:output_file], :all) == msg
  end


  test "Includes metadata in result", context do
    metadata = {:foo, :bar}

    MessageLog.write(context[:pid], "foo", metadata)

    assert_receive {:"$gen_cast", {:write_messagelog_ok, _, _, metadata}}
  end


  test "Returns correct offset, len of writes", context do
    msg = "Hello World!"
    len = byte_size(msg)

    MessageLog.write(context[:pid], msg, nil)

    assert_receive {:"$gen_cast", {:write_messagelog_ok, 0, len, _}}
  end


  test "Writes messages in order", context do
    msgs = 1..5 |> Enum.map(fn _ -> RandomBytes.base16(10) end)

    msgs |> Enum.each(fn msg ->
      MessageLog.write(context[:pid], msg, nil)
      assert_receive {:"$gen_cast", {:write_messagelog_ok, _, _, _}}
    end)

    assert IO.read(context[:output_file], :all) == Enum.join(msgs)
  end

  test "Sends response to correct process", context do
    sample_size = 100
    me = self()

    1..sample_size |> Enum.each(fn n ->
      spawn_link fn ->
        :ok = MessageLog.write(context[:pid], RandomBytes.base16(10), n)
        receive do
          {:"$gen_cast", {:write_messagelog_ok, _, _, n}} -> send me, {:ok, n}
        after
          100 -> send me, {:error, "Sample #{n} timed out"}
        end
      end
    end)

    1..sample_size |> Enum.each(fn n ->
      assert_receive {:ok, n}, 200
    end)
  end


  test "Buffers writes until flush_timeout", context do
    :ok = MessageLog.write(context[:pid], "Foo", :waits)

    refute_receive {:"$gen_cast", {:write_messagelog_ok, _, _, :waits}}, @flush_timeout
    assert_receive {:"$gen_cast", {:write_messagelog_ok, _, _, :waits}}, 5
  end


  test "Buffers writes until buffer_size", context do
    # Send small messages
    :ok = MessageLog.write(context[:pid], "A", :A)
    :ok = MessageLog.write(context[:pid], "B", :B)

    # Ensure flush was not triggered
    refute_received {:"$gen_cast", {:write_messagelog_ok, _, _, :A}}
    refute_received {:"$gen_cast", {:write_messagelog_ok, _, _, :B}}

    # Send message of size buffer_size
    msg_big = Enum.join(List.duplicate("C", @buffer_size))
    :ok = MessageLog.write(context[:pid], msg_big, :big)

    # Buffer was flushed before flush_timeout
    assert_receive  {:"$gen_cast", {:write_messagelog_ok, _, _, :A}}, @flush_timeout-1
    assert_received {:"$gen_cast", {:write_messagelog_ok, _, _, :B}}
    assert_received {:"$gen_cast", {:write_messagelog_ok, _, _, :big}}
    assert log_content(context) == Enum.join ["A", "B", msg_big]
  end


  defp log_content(context) do
     IO.read(context[:output_file], :all)
  end
end
