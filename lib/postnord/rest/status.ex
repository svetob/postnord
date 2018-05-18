defmodule Postnord.Rest.Status do
  def status() do
    {:ok, status} = Poison.encode(%{status: "ok"})
    {:ok, 200, status}
  end
end
