defmodule Postnord.RPC.Client.Rest do
  @behaviour Postnord.RPC.Client

  # TODO Genserver with persistent connection

  def replicate(_pid, _partition, id, timestamp, message, timeout \\ 5_000) do

  end

  def tombstone(_pid, _partition, id, timeout \\ 5_000) do

  end

  def flush(_pid, _queue, timeout \\ 5_000) do
    
  end
end
