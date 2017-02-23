defmodule Postnord.Partition.Consumer.Marks do
  alias Postnord.Partition.Consumer.Marks

  @moduledoc """
  Sent items are 'marked for death' in-memory. Marked items will not be sent to
  the consumer. Marks have an expiry time, and are not persisted to disk.
  """

  defstruct ids: MapSet.new(),
            timeouts: []

  @expiry 60 * 1000

  @doc """
  Return a new empty collection of marks.
  """
  def new do
    %Marks{}
  end

  @doc """
  Add id to marks and set a time for it to expire.
  """
  def add(marks, id, expiry \\ @expiry) do
    %Marks{ids: MapSet.put(marks.ids, id),
           timeouts: marks.timeouts ++ [{id, Postnord.now() + expiry}]} #TODO O(n) operation, is there a Queue in elixir?
  end

  @doc """
  Check if id is currently marked
  """
  def member?(marks, id) do
    marks.ids |> MapSet.member?(id)
  end

  @doc """
  Find marks that have expired and should be requeued. Returns their id's and
  an updated Marks struct with them removed.
  """
  def requeue(marks) do
    ids = marks.timeouts |> requeue_find()
    {ids, requeue_clear_ids(ids, marks)}
  end

  defp requeue_find([]), do: []
  defp requeue_find([{id, timeout} | tl]) do
    if timeout < Postnord.now() do
      [id | requeue_find(tl)]
    else
      []
    end
  end

  defp requeue_clear_ids(ids, marks) do
    %Marks{timeouts: marks.timeouts |> drop(length(ids)),
           ids: MapSet.difference(marks.ids, MapSet.new(ids))}
  end

  defp drop([], _), do: []
  defp drop(list, 0), do: list
  defp drop([hd | tl], n), do: drop(tl, n - 1)
end
