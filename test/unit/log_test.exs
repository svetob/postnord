defmodule Postnord.Test.Log.Writer do
  use ExUnit.Case, async: false

  require Logger
  alias Postnord.Log.Writer
  alias Postnord.Log.Writer.State

  @moduledoc """
  Unit tests for Log writer.
  Writes log entries to a test file and verifies correct content is written
  with correct properties.
  """

  @flush_timeout 20
  @flush_timeout_us @flush_timeout * 1000
  @buffer_size 128

  @path :postnord |> Application.get_env(:data_path) |> Path.join("unit")
  @path_log @path |> Path.join("log")

  setup do
    {:ok, pid} = Writer.start_link(build_state())
    {:ok, file} = File.open(@path_log)

    on_exit(fn ->
      # Remove output files after each test
      File.rm(@path_log)
      File.rmdir(@path)
    end)

    [pid: pid, output_file: file]
  end

  test "Writes entry to file", context do
    entry = random_entry()

    {:ok, 0, 32} = Writer.write(context[:pid], entry)

    assert log_content(context) == entry
  end

  test "Buffers writes until flush_timeout", context do
    me = self()

    spawn_link(fn ->
      {:ok, 0, 32} = Writer.write(context[:pid], random_entry())
      send(me, :write_ok)
    end)

    refute_receive :write_ok, @flush_timeout - 1
    assert_receive :write_ok, @flush_timeout
  end

  test "Buffers writes until buffer_size", context do
    pid = context[:pid]

    # Send single message
    {time_a, _} =
      :timer.tc(fn ->
        {:ok, _, 32} = Writer.write(pid, random_entry())
      end)

    # Send single message
    {time_b, _} =
      :timer.tc(fn ->
        {:ok, _, 32} = Writer.write(pid, random_entry())
      end)

    # Write enough messages to exceed buffer size
    {time_large, _} =
      :timer.tc(fn ->
        1..4
        |> Enum.map(fn _ ->
          Task.async(fn -> Writer.write(pid, random_entry()) end)
        end)
        |> Enum.each(fn t ->
          {:ok, _, 32} = Task.await(t)
        end)
      end)

    # Send single message
    {time_c, _} =
      :timer.tc(fn ->
        {:ok, _, 32} = Writer.write(pid, random_entry())
      end)

    assert time_a >= @flush_timeout_us
    assert time_b >= @flush_timeout_us
    assert time_c >= @flush_timeout_us

    assert time_large < @flush_timeout_us
  end

  test "Sends response to correct process", context do
    sample_size = 100
    me = self()

    # Spawn sample_size writes, assert response indicates correct size
    Enum.each(1..sample_size, fn byte_size ->
      spawn_link(fn ->
        {:ok, _, entry_size} = Writer.write(context[:pid], random_entry(byte_size))
        assert entry_size == byte_size
        send(me, {:ok, byte_size})
      end)
    end)

    # Assert all processes receive response
    Enum.each(1..sample_size, fn byte_size ->
      assert_receive {:ok, ^byte_size}, 2000
    end)
  end

  test "Returns correct offset and length for writes", context do
    pid = context[:pid]

    {:ok, 0, 32} = Writer.write(pid, random_entry())

    {:ok, 32, 32} = Writer.write(pid, random_entry())

    1..4
    |> Enum.map(fn _ ->
      Task.async(fn -> Writer.write(pid, random_entry()) end)
    end)
    |> Enum.each(fn t ->
      {:ok, offset, 32} = Task.await(t)
      assert offset >= 64
    end)

    {:ok, 192, 32} = Writer.write(pid, random_entry())
  end

  test "New logger towards existing file writes at correct offset", context do
    entry_a = random_entry()
    entry_b = random_entry()

    {:ok, 0, 32} = Writer.write(context[:pid], entry_a)

    {:ok, pid_b} = Writer.start_link(build_state())

    {:ok, 32, 32} = Writer.write(pid_b, entry_b)

    assert log_content(context) == entry_a <> entry_b
  end

  defp log_content(context) do
    IO.binread(context[:output_file], :all)
  end

  defp random_entry(size \\ 32) do
    :crypto.strong_rand_bytes(size)
  end

  defp build_state do
    %State{
      path: @path_log,
      buffer_size: @buffer_size,
      flush_timeout: @flush_timeout
    }
  end
end
