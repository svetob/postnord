defmodule Postnord.Test.Cluster.RequestCoordination do
  use ExUnit.Case, async: false

  require Logger

  alias TestUtil.Rest


  @moduledoc """
  Cluster tests which verify requests are correctly coordinated
  accross multiple nodes.
  """

  @ports [2011, 2012, 2013]
  @req_opts [timeout: 10_000_000]

  setup_all do
    cluster = TestUtil.Cluster.create(@ports)
    Process.sleep(1_000)

    on_exit fn ->
      TestUtil.Cluster.teardown(cluster)
    end

    uris = @ports |> Enum.map(fn p -> "http://localhost:#{p}" end)

    [cluster: cluster, uris: uris]
  end

  setup context do
    flush_cluster(context[:uris])
    :ok
  end

  def flush_cluster(uris) do
    Logger.info "Flushing cluster"
    uri = hd uris
    resp_flush = Rest.flush_queue(uri)
    assert resp_flush.status_code == 202
  end


  test "can write to one node then read from another", context do
    [uri_a, uri_b, _] = context[:uris]

    message = RandomBytes.base62(1024)

    # Write message to node A
    resp_post = Rest.post_message(uri_a, message)
    assert resp_post.status_code == 201

    # Read message from node B
    resp_get = Rest.get_message(uri_b)
    assert resp_get.status_code == 200
    assert resp_get.body == message
  end

  test "cannot see message from any node after queue flush", context do
    [uri_a, uri_b, _] = context[:uris]

    message = RandomBytes.base62(1024)

    # Write message to node A
    resp_post = Rest.post_message(uri_a, message)
    assert resp_post.status_code == 201

    # Flush queue from node B
    resp_flush = Rest.flush_queue(uri_b)
    assert resp_flush.status_code == 202

    # Read from any node returns no content
    Enum.each(context[:uris], fn node ->
      resp_get = Rest.get_message(uri_b)
      assert resp_get.status_code == 204
    end)
  end

  # TODO Requires message holds
  # test "cannot see a message from any node once it has been read", context do
  #   [uri_a, uri_b, _] = context[:uris]
  #
  #   message = RandomBytes.base62(1024)
  #
  #   # Write message to node A
  #   resp_post = Rest.post_message(uri_a, message)
  #   assert resp_post.status_code == 201
  #
  #   # Read message from node B
  #   resp_get = Rest.get_message(uri_b)
  #   assert resp_get.status_code == 200
  #   assert resp_get.body == message
  #
  #   # TODO Sleep should not be necessary
  #   Process.sleep(100)
  #
  #   Enum.each(context[:uris], fn node ->
  #     resp_get = Rest.get_message(uri_b)
  #     assert resp_get.status_code == 204
  #   end)
  # end

  test "cannot see a message from any node once it has been accepted", context do
    [uri_a, uri_b, _] = context[:uris]

    message = RandomBytes.base62(1024)

    # Write message to node A
    resp_post = Rest.post_message(uri_a, message)
    assert resp_post.status_code == 201

    # Read message from node B
    resp_get = Rest.get_message(uri_b)
    assert resp_get.status_code == 200
    assert resp_get.body == message
    id = Rest.headers_message_id(resp_get.headers)

    # Accept message from node B
    resp_accept = Rest.accept_message(uri_b, id)
    assert resp_accept.status_code == 202

    # TODO Sleep should not be necessary
    Process.sleep(100)

    Enum.each(context[:uris], fn node ->
      resp_get = Rest.get_message(uri_b)
      assert resp_get.status_code == 204
    end)
  end
end
