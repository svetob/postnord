defmodule Postnord.Test.Integration.REST do
  use ExUnit.Case, async: false

  alias Postnord.Id

  @moduledoc """
  Test REST interface and functionality.
  """

  setup do
    %HTTPotion.Response{status_code: 202} = "queue/q/flush" |> uri() |> HTTPotion.post()
    :ok
  end

  test "can check server status" do
    %HTTPotion.Response{body: body, status_code: 200} = "_status" |> uri() |> HTTPotion.get()
    assert Poison.decode!(body)["status"] == "ok"
  end

  test "can write, read and confirm a message" do
    message = random_message()

    # Post message
    resp_post = post_message(message)
    assert resp_post.status_code == 201

    # Get message
    resp_get = get_message()
    assert resp_get.status_code == 200
    assert resp_get.body == message
    id = resp_get.headers.hdrs["message_id"]

    # Confirm message id
    resp_accept = accept_message(id)
    assert resp_accept.status_code == 202

    # Get message returns 204 No Content
    resp_get = get_message()
    assert resp_get.status_code == 204
    assert resp_get.headers.hdrs["message_id"] == nil
  end

  test "204 when attempting to get message from empty queue" do
    resp_get = get_message()
    assert resp_get.status_code == 204
    assert resp_get.headers.hdrs["message_id"] == nil
  end

  test "405 for unsupported operations" do
    resp_put = HTTPotion.put(uri("queue/q/message"), [body: "Hello REST!"])
    assert resp_put.status_code == 405
  end

  test "can flush queue" do
    message = random_message()

    # Post message
    resp_post = post_message(message)
    assert resp_post.status_code == 201

    # Flush
    resp_flush = flush_queue()
    assert resp_flush.status_code == 202

    # Get message returns 204 No Content
    resp_get = get_message()
    assert resp_get.status_code == 204
    assert resp_get.headers.hdrs["message_id"] == nil
  end

  test "can replicate a message" do
    timestamp = :erlang.system_time(:nanosecond)
    id = Id.message_id_encode(Id.message_id())
    message = random_message()

    # Replicate message
    resp_replicate = replicate_message(id, timestamp, message)
    assert resp_replicate.status_code == 201

    # Get returns replicated message
    resp_get = get_message()
    assert resp_get.status_code == 200
    assert resp_get.body == message
    assert resp_get.headers.hdrs["message_id"] == id
  end

  test "can tombstone a message" do
    timestamp = :erlang.system_time(:nanosecond)
    id = Id.message_id_encode(Id.message_id())
    message = random_message()

    # Replicate message
    resp_replicate = replicate_message(id, timestamp, message)
    assert resp_replicate.status_code == 201

    # Tombstone dat shit
    resp_tombstone = tombstone_message(id)
    assert resp_tombstone.status_code == 202

    # Get message returns 204 No Content
    resp_get = get_message()
    assert resp_get.status_code == 204
    assert resp_get.headers.hdrs["message_id"] == nil
  end

  def uri(path) do
    port = Application.get_env(:postnord, :port)
    "localhost:#{port}/#{path}"
  end

  def get_message do
    "queue/q/message" |> uri() |> HTTPotion.get()
  end

  def post_message(message) do
    "queue/q/message" |> uri() |> HTTPotion.post([body: message])
  end

  def accept_message(id) do
    "queue/q/message/#{id}/accept" |> uri() |> HTTPotion.post()
  end

  def replicate_message(id, timestamp, message) do
    "queue/q/message/#{id}/timestamp/#{timestamp}/replicate"
    |> uri()
    |> HTTPotion.post([body: message])
  end

  def tombstone_message(id) do
    "queue/q/message/#{id}/tombstone" |> uri() |> HTTPotion.post()
  end

  def flush_queue() do
    "queue/q/flush" |> uri() |> HTTPotion.post()
  end

  def random_message do
    RandomBytes.base62(1024)
  end
end
