defmodule Postnord.Test.Integration.REST do
  use ExUnit.Case, async: false

  @moduledoc """
  Test REST interface and functionality.
  """

  test "can check server status" do
    %HTTPotion.Response{body: body, status_code: 200} = HTTPotion.get(uri("/"))
    assert Poison.decode!(body)["status"] == "ok"
  end

  test "can write, read and confirm a message" do
    resp_post = HTTPotion.post(uri("/queue/q/message/"), [body: "Hello REST!"])
    assert resp_post.status_code == 201

    resp_get = HTTPotion.get(uri("/queue/q/message/"))
    assert resp_get.status_code == 200

    # TODO Confirm
  end

  test "204 when attempting to get message from empty queue" do
    resp_get = HTTPotion.get(uri("/queue/q/message/"))
    assert resp_get.status_code == 204
  end

  test "405 for unsupported operations" do
    resp_put = HTTPotion.put(uri("/queue/q/message/"), [body: "Hello REST!"])
    assert resp_put.status_code == 405
  end

  test "can flush queue" do

  end

  test "can replicate a message" do

  end

  test "can tombstone a message" do

  end

  def uri(path) do
    port = Application.get_env(:postnord, :port)
    "localhost:#{port}#{path}"
  end
end
