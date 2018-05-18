defmodule Postnord.Test.Integration.REST do
  use ExUnit.Case, async: false

  alias Postnord.Id
  alias TestUtil.Rest

  @moduledoc """
  Test REST interface and functionality.
  """

  setup do
    %HTTPoison.Response{status_code: 202} = "queue/q/flush"
      |> uri()
      |> HTTPoison.post!("")

    :ok
  end

  test "can check server status" do
    %HTTPoison.Response{body: body, status_code: 200} = "_status"
      |> uri()
      |> HTTPoison.get!()
    assert Poison.decode!(body)["status"] == "ok"
  end

  test "can write, read and confirm a message" do
    message = Rest.random_message()

    # Post message
    resp_post = Rest.post_message(host(), message)
    assert resp_post.status_code == 201

    # Get message
    resp_get = Rest.get_message(host())
    assert resp_get.status_code == 200
    assert resp_get.body == message
    id = headers_message_id(resp_get.headers)

    # Confirm message id
    resp_accept = Rest.accept_message(host(), id)
    assert resp_accept.status_code == 202

    # Get message returns 204 No Content
    resp_get = Rest.get_message(host())
    assert resp_get.status_code == 204
    assert headers_message_id(resp_get.headers) == nil
  end

  test "204 when attempting to get message from empty queue" do
    resp_get = Rest.get_message(host())
    assert resp_get.status_code == 204
    assert headers_message_id(resp_get.headers) == nil
  end

  test "405 for unsupported operations" do
    resp_put = HTTPoison.put!(uri("queue/q/message"), "Foo")
    assert resp_put.status_code == 405
  end

  test "can flush queue" do
    message = Rest.random_message()

    # Post message
    resp_post = Rest.post_message(host(), message)
    assert resp_post.status_code == 201

    # Flush
    resp_flush = Rest.flush_queue(host())
    assert resp_flush.status_code == 202

    # Get message returns 204 No Content
    resp_get = Rest.get_message(host())
    assert resp_get.status_code == 204
    assert headers_message_id(resp_get.headers) == nil
  end

  test "can replicate a message" do
    timestamp = :erlang.system_time(:nanosecond)
    id = Id.message_id_encode(Id.message_id())
    message = Rest.random_message()

    # Replicate message
    resp_replicate = Rest.replicate_message(host(), id, timestamp, message)
    assert resp_replicate.status_code == 201

    # Get returns replicated message
    resp_get = Rest.get_message(host())
    assert resp_get.status_code == 200
    assert resp_get.body == message
    assert headers_message_id(resp_get.headers) == id
  end

  test "can tombstone a message" do
    timestamp = :erlang.system_time(:nanosecond)
    id = Id.message_id_encode(Id.message_id())
    message = Rest.random_message()

    # Replicate message
    resp_replicate = Rest.replicate_message(host(), id, timestamp, message)
    assert resp_replicate.status_code == 201

    # Tombstone dat shit
    resp_tombstone = Rest.tombstone_message(host(), id)
    assert resp_tombstone.status_code == 202

    # Get message returns 204 No Content
    resp_get = Rest.get_message(host())
    assert resp_get.status_code == 204
    assert headers_message_id(resp_get.headers) == nil
  end

  def host() do
    port = Application.get_env(:postnord, :port)
    "localhost:#{port}"
  end
end
