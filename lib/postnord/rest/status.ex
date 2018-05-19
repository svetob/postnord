defmodule Postnord.Rest.Status do
  @moduledoc """
  REST Status request handler.
  """

  def status() do
    {:ok, status} = Poison.encode(%{status: "ok"})
    {:ok, 200, status}
  end
end
