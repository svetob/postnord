defmodule Postnord.Partition do
  require Logger
  use GenServer

  def start_link(path, opts \\ []) do
    GenServer.start_link(__MODULE__, path, opts)
  end

  def init(path) do
    import Supervisor.Spec, warn: false

    children = [
      worker(Postnord.MessageLog, [path <> "/message.log", [name: Postnord.MessageLog]]),
      worker(Postnord.IndexLog, [path <> "/index.log", [name: Postnord.IndexLog]])
    ]

    Supervisor.start_link(children, [strategy: :one_for_one])
    {:ok, nil}
  end

  @doc """
  Writes a single message to the partition.
  """
  @spec write_message(pid, pid, binary()) :: {:ok} | {:error, any()}
  def write_message(pid, caller, bytes, timeout \\ 5_000) do
    GenServer.cast(pid, {:write, caller, bytes})
    receive do
      {:write_ok} -> :ok
    after
      timeout -> {:error, :timeout}
    end
  end

  def handle_cast({:write, caller, bytes}, nil) do
    id = Postnord.now(:nanosecond) # TODO: ID Generator
    Postnord.MessageLog.write(Postnord.MessageLog, self(), bytes, {caller, id})
    {:noreply, nil}
  end

  def handle_cast({:write_messagelog_ok, offset, len, {caller, id}}, nil) do
    entry = %Postnord.IndexLog.Entry{id: id, offset: offset, len: len}
    Postnord.IndexLog.write(Postnord.IndexLog, self(), entry, {caller})
    {:noreply, nil}
  end

  def handle_cast({:write_indexlog_ok, {caller}}, nil) do
    send caller, {:write_ok}
    {:noreply, nil}
  end

end
