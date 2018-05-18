defmodule Postnord.Cluster.State do

  @moduledoc """
  Locally shared to-be-RAFTed Cluster state.
  """

  defstruct hosts: %{},
            my_id: ""

  defp new(my_id) do
    %__MODULE__{my_id: my_id}
  end

  def start_link(my_id) do
    Agent.start_link(fn -> new(my_id) end, name: __MODULE__)
    hosts = Application.get_env(:postnord, :replica_nodes, [])
    hosts |> Enum.each(fn {id, uri} -> add_host(id, uri) end)
    add_host(my_id, nil)
  end

  def get() do
    Agent.get(__MODULE__, fn state -> state end)
  end

  def add_host(host_id, host_uri) do
    Agent.update(__MODULE__, fn state ->
      %__MODULE__{state | hosts: state.hosts |> Map.put(host_id, host_uri)}
    end)
  end

end
