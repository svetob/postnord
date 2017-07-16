defmodule Postnord.Rest.Route.Status do
  @moduledoc """
  Returns current node status
  """

  def init(req, opts) do
    {:cowboy_rest, req, opts}
  end

  def content_types_provided(req, state) do
  	{[
  		{"application/json", :status}
  	], req, state}
  end

  def status(req, state) do
    response = %{
      status: :ok,
      state: Postnord.Cluster.State.get()
    } |> Poison.encode!()
  	{response, req, state}
  end
end
