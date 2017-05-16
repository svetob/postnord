defmodule Postnord.Test.Consumer.Partition do
  use ExUnit.Case, async: false

  require Logger
  alias Postnord.IndexLog.Entry, as: Entry
  alias Postnord.Consumer.PartitionConsumer.State, as: State
  alias Postnord.Consumer.PartitionConsumer

  @moduledoc """
  Unit tests for partition consumer.

  The tests create readable partition files, performs serial read operations and
  verifies the results.
  """

  # TODO: Rework tests later when proper tombstoning and requeueing is in effect.

  @path Application.get_env(:postnord, :test_data_path)
  @path_message_log Path.join(@path, "message.log")
  @path_index_log Path.join(@path, "index.log")

  setup do
    {:ok, pid} = PartitionConsumer.start_link(%State{path: @path})

    # Create output directory
    :ok = @path
    |> Path.dirname()
    |> File.mkdir_p()

    on_exit fn ->
      # Remove output files after each test
      File.rm(@path_message_log)
      File.rm(@path_index_log)
      File.rmdir(@path)
    end

    [pid: pid]
  end

  test "Reads value from file", context do
    msgs = ["foo"]
    write_data(msgs, entries_for(msgs))

    {:ok, _id, "foo"} = PartitionConsumer.read(context[:pid])
  end

  test "Reads same value from file until accepted", context do
    msgs = ["foo", "bar"]
    write_data(msgs, entries_for(msgs))

    {:ok, id_a, "foo"} = PartitionConsumer.read(context[:pid])
    assert {:ok, id_a, "foo"} == PartitionConsumer.read(context[:pid])
    assert :ok == PartitionConsumer.accept(context[:pid], id_a)
    {:ok, id_b, "bar"} = PartitionConsumer.read(context[:pid])
    assert {:ok, id_b, "bar"} == PartitionConsumer.read(context[:pid])
  end

  test "Reads values from file in order", context do
    msgs = ["foo", "bar", "cats"]
    write_data(msgs, entries_for(msgs))

    {:ok, id_a, "foo"} = PartitionConsumer.read(context[:pid])
    assert :ok == PartitionConsumer.accept(context[:pid], id_a)
    {:ok, id_b, "bar"} = PartitionConsumer.read(context[:pid])
    assert :ok == PartitionConsumer.accept(context[:pid], id_b)
    {:ok, id_c, "cats"} = PartitionConsumer.read(context[:pid])
    assert :ok == PartitionConsumer.accept(context[:pid], id_c)
  end

  test "Returns :empty if file is empty", context do
    write_data([], entries_for([]))

    assert :empty == PartitionConsumer.read(context[:pid])
  end

  test "Returns :empty if no more entries exist", context do
    msgs = ["foo"]
    write_data(msgs, entries_for(msgs))

    {:ok, id, "foo"} = PartitionConsumer.read(context[:pid])
    assert :ok == PartitionConsumer.accept(context[:pid], id)
    assert :empty == PartitionConsumer.read(context[:pid])
    assert :empty == PartitionConsumer.read(context[:pid])
    assert :empty == PartitionConsumer.read(context[:pid])
  end

  test "Returns :empty if data for entry not written yet", context do
    msgs = ["foo"]
    entries = entries_for ["foo", "bar"]
    write_data(msgs, entries)

    {:ok, id, "foo"} = PartitionConsumer.read(context[:pid])
    assert :ok == PartitionConsumer.accept(context[:pid], id)
    assert :empty == PartitionConsumer.read(context[:pid])
  end

  test "Returns :empty if entry for data not written yet", context do
    msgs = ["foo", "bar"]
    entries = entries_for(["foo"])
    write_data(msgs, entries)

    {:ok, id, "foo"} = PartitionConsumer.read(context[:pid])
    assert :ok == PartitionConsumer.accept(context[:pid], id)
    assert :empty == PartitionConsumer.read(context[:pid])
  end

  test "Returns :empty if data for entry only partially written", context do
    msgs = ["foo", "ba"]
    entries = entries_for(["foo", "bar"])
    write_data(msgs, entries)

    {:ok, id, "foo"} = PartitionConsumer.read(context[:pid])
    assert :ok == PartitionConsumer.accept(context[:pid], id)
    assert :empty == PartitionConsumer.read(context[:pid])
  end

  test "Returns :error if message log does not exist", context do
    write_data(["foo"], entries_for(["foo"]))
    File.rm(@path_message_log)

    assert {:error, :enoent} == PartitionConsumer.read(context[:pid])
  end

  test "Returns :error if index log does not exist", context do
    write_data(["foo"], entries_for(["foo"]))
    File.rm(@path_index_log)

    assert {:error, :enoent} == PartitionConsumer.read(context[:pid])
  end


  defp entries_for(messages) do
    Enum.reduce(messages, [], fn (m, acc) ->
      id = Postnord.IdGen.id()
      case acc do
        [] -> [%Entry{id: id, offset: 0, len: byte_size m}]
        _ ->
          prev = List.last(acc)
          acc ++ [%Entry{id: id,
                        offset: prev.offset + prev.len,
                        len: byte_size m}]
      end
    end)
  end

  defp write_data(message_log, index_log) do
    msgbytes = message_log
      |> Enum.join
      |> :erlang.iolist_to_binary
    indexbytes = index_log
      |> Enum.map(&Entry.as_bytes/1)
      |> Enum.join
      |> :erlang.iolist_to_binary

    File.write(@path_message_log, msgbytes)
    File.write(@path_index_log, indexbytes)
  end
end
