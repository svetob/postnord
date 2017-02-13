defmodule Postnord.Test.MessageLog do
  use ExUnit.Case, async: false

  require Logger
  alias Postnord.MessageLog
  alias Postnord.MessageLog.State

  setup do
    path = Application.get_env(:postnord, :test_data_path)
    file_path = Path.join(path, "message.log")
    {:ok, message_log} = MessageLog.start_link(%State{path: path})
    {:ok, file} = File.open(file_path, [:binary, :read])

    on_exit fn ->
      File.rm(file_path)
      File.rmdir(path)
    end

    [pid: message_log, output_file: file]
  end

  test "Writes message to file", context do
    msg = "Hello World!"
    MessageLog.write(context[:pid], msg, nil)

    assert_receive {:"$gen_cast", {:write_messagelog_ok, _, _, _}}, 2_000
    assert IO.read(context[:output_file], :all) == msg
  end
  #
  #
  # test "Includes metadata in result" do
  #
  # end
  #
  # test "Sends response to correct process" do
  #
  # end
  #
  # test "Buffers writes until flush_timeout" do
  #
  # end
  #
  # test "Buffers writes until buffer_size" do
  #
  # end
end
