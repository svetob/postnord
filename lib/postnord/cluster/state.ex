defmodule Postnord.Cluster.State do

  @moduledoc """
  Locally shared RAFT Cluster state.
  """

  defstruct hosts: [],
            my_id: ""

  def start_link(my_id) do
    Agent.start_link(fn ->
      %__MODULE__{my_id: my_id, hosts: [{my_id, nil}]}
    end, name: __MODULE__)
  end

  def get() do
    Agent.get(__MODULE__, fn state -> state end)
  end

end
