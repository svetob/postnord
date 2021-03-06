defmodule Postnord.Perftest do
  alias Postnord.RPC.Coordinator
  require Logger

  @moduledoc """
  Common performance test functionality for mix tasks.
  """

  @doc """
  Launch Postnord application
  """
  def launch do
    Mix.Project.config()
    |> Keyword.get(:deps)
    |> Enum.each(fn dependency ->
      Application.ensure_all_started(elem(dependency, 0))
    end)

    Postnord.main([])
  end

  @doc """
  Write-test, which writes the specified size and nr of messages, and measures
  write throughput.
  """
  def write_test(msgbytes, writers, entries) do
    msg = :erlang.iolist_to_binary(RandomBytes.base16(Integer.floor_div(msgbytes, 2)))
    entries_each = div(entries, writers)

    Logger.info(
      "Write test: #{writers} writers, #{entries_each} entries each at #{
        Float.to_string(byte_size(msg) / 1024)
      }kb each"
    )

    start = Postnord.now()

    write_test_execute(writers, entries_each, msg)

    wrote_entries = entries_each * writers
    wrote_mb = byte_size(msg) * wrote_entries / (1024 * 1024)
    wrote_time_s = (Postnord.now() - start) / 1000

    Logger.info(
      "Wrote " <>
        "#{Float.to_string(wrote_mb)}Mb, " <>
        "#{Integer.to_string(wrote_entries)} entries in " <>
        "#{Float.to_string(wrote_time_s)}s at " <>
        "#{Float.to_string(wrote_entries / wrote_time_s)}dps, #{
          Float.to_string(wrote_mb / wrote_time_s)
        }Mbps"
    )
  end

  defp write_test_execute(writers, entries_each, msg) do
    1..writers
    |> Enum.map(fn _ ->
      from = self()
      spawn_link(fn -> write(from, msg, entries_each) end)
    end)
    |> Enum.each(fn _ ->
      receive do
        :ok -> :ok
      end
    end)
  end

  defp write(from, _msg, 0) do
    send(from, :ok)
  end

  defp write(from, msg, remain) do
    case Coordinator.write_message(nil, msg) do
      :ok ->
        write(from, msg, remain - 1)

      {:error, reason} ->
        raise {:error, reason}
    end
  end

  @doc """
  Read-test, which writes the specified nr of messages, and measures read
  throughput.
  """
  def read_test(readers, entries) do
    entries_each = div(entries, readers)

    Logger.info("Read test: #{readers} readers reading #{entries_each} entries each")
    start = Postnord.now()

    read_test_execute(readers, entries_each)

    read_entries = entries_each * readers
    read_time_s = (Postnord.now() - start) / 1000

    Logger.info(
      "Read " <>
        "#{Integer.to_string(read_entries)} entries in " <>
        "#{Float.to_string(read_time_s)}s at " <>
        "#{Float.to_string(read_entries / read_time_s)}dps"
    )
  end

  defp read_test_execute(readers, entries) do
    1..readers
    |> Enum.map(fn _ ->
      from = self()
      spawn_link(fn -> read(from, entries) end)
    end)
    |> Enum.each(fn _ ->
      receive do
        :ok -> :ok
      end
    end)
  end

  defp read(from, 0) do
    send(from, :ok)
  end

  defp read(from, remain) do
    case Coordinator.read_message(nil) do
      {:ok, id, _} -> accept(from, remain, id)
      :empty -> read(from, remain)
      {:error, reason} -> raise {:error, reason}
    end
  end

  defp accept(from, remain, id) do
    case Coordinator.confirm_accept(nil, id) do
      :ok -> read(from, remain - 1)
      :noop -> read(from, remain)
    end
  end
end
