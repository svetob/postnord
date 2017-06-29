defmodule Postnord.Consumer.PartitionConsumer.Holds do
  alias Postnord.Consumer.PartitionConsumer.State
  alias Postnord.Consumer.Hold

  @spec request_hold(String.t, String.t, integer()) :: boolean()
  def request_hold(host, id, time \\ 5_000) do
    
  end

end
