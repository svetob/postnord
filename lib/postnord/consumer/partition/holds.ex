defmodule Postnord.Consumer.Partition.Holds do
  @moduledoc """
  Holds structure.
  """

  @expiry_time 5_000

  def new do
    Map.new()
  end

  def hold(holds, id) do
    Map.put(holds, id, Postnord.now() + @expiry_time)
  end

  def held?(holds, id) do
    case Map.get(holds, id) do
      nil ->
        false

      expiry ->
        expiry > Postnord.now()
    end
  end

  def release(holds, id) do
    Map.delete(holds, id)
  end

  @doc """
  Find and remove expired holds.
  """
  def cleanup(holds) do
    now = Postnord.now()

    expired_ids =
      holds
      |> Enum.filter(fn {_id, expiry} -> expiry <= now end)
      |> Enum.map(fn {id, _expiry} -> id end)

    holds = Map.drop(holds, expired_ids)

    {holds, expired_ids}
  end
end
