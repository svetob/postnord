defmodule Postnord.Test.Consumer.Partition.Marks do
  use ExUnit.Case, async: true

  alias Postnord.Partition.Consumer.Marks

  test "requeue removes items to be requeued and retains rest" do
      now = Postnord.now()
      marks = %Marks{ids: [1,2,3] |> MapSet.new(),
                     timeouts: [{1, now - 200}, {2, now - 100}, {3, now + 100}]}
      {requeue, marks_updated} = Marks.requeue(marks)

      assert requeue == [1,2]
      assert marks_updated.ids == [3] |> MapSet.new()
      assert marks_updated.timeouts == [{3, now + 100}]
  end

  test "requeue removes nothing if nothing to be requeued" do
    now = Postnord.now()
    marks = %Marks{ids: [1,2,3] |> MapSet.new(),
                   timeouts: [{1, now + 100}, {2, now + 200}, {3, now + 300}]}
    {requeue, marks_updated} = Marks.requeue(marks)

    assert requeue == []
    assert marks_updated.ids == [1,2,3] |> MapSet.new()
    assert length(marks_updated.timeouts) == 3
  end

  test "requeue does nothing if marks are empty" do
    {requeue, marks_updated} = Marks.requeue(%Marks{})

    assert requeue == []
    assert marks_updated == %Marks{}
  end
end
