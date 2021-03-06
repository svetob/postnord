defmodule Postnord.Test.Consumer.Partition do
  use ExUnit.Case, async: false

  require Logger

  alias Postnord.Partition.MessageIndex
  alias Postnord.Consumer.Partition.State, as: State
  alias Postnord.Consumer.Partition

  @moduledoc """
  Unit tests for partition consumer.

  The tests create readable partition files, performs serial read operations and
  verifies the results.
  """

  # TODO: Rework tests later when proper tombstoning and requeueing is in effect.

  @path :postnord |> Application.get_env(:data_path) |> Path.join("unit")
  @path_message_log @path |> Path.join("message.log")
  @path_index_log @path |> Path.join("index.log")

  setup do
    {:ok, pid} = Partition.start_link(%State{path: @path})

    # Create output directory
    :ok = File.mkdir_p(@path <> "/")

    on_exit(fn ->
      # Remove output files after each test
      File.rm(@path_message_log)
      File.rm(@path_index_log)
      File.rmdir(@path)
    end)

    [pid: pid]
  end

  test "Reads value from file", context do
    msgs = ["foo"]
    write_data(msgs, entries_for(msgs))

    {:ok, _id, "foo"} = Partition.read(context[:pid])
  end

  test "Reads same value from file until accepted", context do
    msgs = ["foo", "bar"]
    write_data(msgs, entries_for(msgs))

    {:ok, id_a, "foo"} = Partition.read(context[:pid])
    assert {:ok, id_a, "foo"} == Partition.read(context[:pid])
    assert :ok == Partition.accept(context[:pid], id_a)
    {:ok, id_b, "bar"} = Partition.read(context[:pid])
    assert {:ok, id_b, "bar"} == Partition.read(context[:pid])
  end

  test "Reads values from file in order", context do
    msgs = ["foo", "bar", "cats"]
    write_data(msgs, entries_for(msgs))

    {:ok, id_a, "foo"} = Partition.read(context[:pid])
    assert :ok == Partition.accept(context[:pid], id_a)
    {:ok, id_b, "bar"} = Partition.read(context[:pid])
    assert :ok == Partition.accept(context[:pid], id_b)
    {:ok, id_c, "cats"} = Partition.read(context[:pid])
    assert :ok == Partition.accept(context[:pid], id_c)
  end

  test "Returns :empty if file is empty", context do
    write_data([], entries_for([]))

    assert :empty == Partition.read(context[:pid])
  end

  test "Returns :empty if no more entries exist", context do
    msgs = ["foo"]
    write_data(msgs, entries_for(msgs))

    {:ok, id, "foo"} = Partition.read(context[:pid])
    assert :ok == Partition.accept(context[:pid], id)
    assert :empty == Partition.read(context[:pid])
    assert :empty == Partition.read(context[:pid])
    assert :empty == Partition.read(context[:pid])
  end

  test "Returns {:error, :eof} if message bytes not found for entry", context do
    msgs = ["foo"]
    entries = entries_for(["foo", "bar"])
    write_data(msgs, entries)

    {:ok, id, "foo"} = Partition.read(context[:pid])
    assert :ok == Partition.accept(context[:pid], id)
    assert {:error, :eof} == Partition.read(context[:pid])
  end

  test "Returns :empty if entry for data not written yet", context do
    msgs = ["foo", "bar"]
    entries = entries_for(["foo"])
    write_data(msgs, entries)

    {:ok, id, "foo"} = Partition.read(context[:pid])
    assert :ok == Partition.accept(context[:pid], id)
    assert :empty == Partition.read(context[:pid])
  end

  test "Returns {:error, _} if data for entry only partially written", context do
    msgs = ["foo", "ba"]
    entries = entries_for(["foo", "bar"])
    write_data(msgs, entries)

    {:ok, id, "foo"} = Partition.read(context[:pid])
    assert :ok == Partition.accept(context[:pid], id)
    {:error, _reason} = Partition.read(context[:pid])
  end

  test "Returns :error if message log does not exist", context do
    write_data(["foo"], entries_for(["foo"]))
    File.rm(@path_message_log)

    assert {:error, :enoent} == Partition.read(context[:pid])
  end

  test "Returns :error if index log does not exist", context do
    write_data(["foo"], entries_for(["foo"]))
    File.rm(@path_index_log)

    assert {:error, :enoent} == Partition.read(context[:pid])
  end

  defp entries_for(messages) do
    Enum.reduce(messages, [], fn m, acc ->
      id = Postnord.Id.message_id()

      case acc do
        [] ->
          [%MessageIndex{id: id, offset: 0, len: byte_size(m), timestamp: Postnord.now()}]

        _ ->
          prev = List.last(acc)

          acc ++
            [
              %MessageIndex{
                id: id,
                offset: prev.offset + prev.len,
                len: byte_size(m),
                timestamp: Postnord.now()
              }
            ]
      end
    end)
  end

  defp write_data(message_log, index_log) do
    msgbytes =
      message_log
      |> Enum.join()
      |> :erlang.iolist_to_binary()

    indexbytes =
      index_log
      |> Enum.map(&MessageIndex.as_bytes/1)
      |> Enum.join()
      |> :erlang.iolist_to_binary()

    File.write(@path_message_log, msgbytes)
    File.write(@path_index_log, indexbytes)
  end
end
