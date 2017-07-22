defmodule Postnord.Test.Cluster.RequestCoordination do
  use ExUnit.Case, async: false

  @moduledoc """
  Cluster tests which verify requests are correctly coordinated
  accross multiple nodes.
  """

  alias Postnord.GRPC.WriteRequest
  alias Postnord.GRPC.ReadRequest
  alias Postnord.GRPC.ConfirmRequest
  alias Postnord.GRPC.Node

  @req_opts [timeout: 1_000_000]

  setup do
    ports = [2021, 2022, 2023]
    cluster = TestUtil.Cluster.create(ports)
    Process.sleep(1_000)

    on_exit fn ->
      TestUtil.Cluster.teardown(cluster)
    end

    channels = ports |> Enum.map(fn p ->
      {:ok, channel} = GRPC.Stub.connect("localhost:#{p}")
      channel
    end)

    [cluster: cluster, channels: channels]
  end


  test "can write to one node then read from another", context do
    [node_a, node_b, _] = context[:channels]

    message = RandomBytes.base62(1024)

    write_req = WriteRequest.new(message: message)
    write_reply = Node.Stub.write(node_a, write_req, @req_opts)
    assert write_reply.response == :OK

    read_req = ReadRequest.new()
    read_reply = Node.Stub.read(node_b, read_req, @req_opts)
    assert read_reply.response == :OK
    assert read_reply.message == message
  end

  test "cannot see a message from any node once it has been read", context do
    [node_a, node_b, _] = context[:channels]

    message = RandomBytes.base62(1024)

    write_req = WriteRequest.new(message: message)
    write_reply = Node.Stub.write(node_a, write_req, @req_opts)
    assert write_reply.response == :OK

    read_req = ReadRequest.new()
    read_reply = Node.Stub.read(node_b, read_req, @req_opts)
    assert read_reply.response == :OK
    assert read_reply.message == message

    context[:channels] |> Enum.each(fn node ->
      read_reply = Node.Stub.read(node, read_req, @req_opts)
      assert read_reply.response == :EMPTY
    end)
  end

  test "cannot see a message from any node once it has been accepted", context do
    [node_a, node_b, node_c] = context[:channels]

    message = RandomBytes.base62(1024)

    write_req = WriteRequest.new(message: message)
    write_reply = Node.Stub.write(node_a, write_req, @req_opts)
    assert write_reply.response == :OK

    read_req = ReadRequest.new()
    read_reply = Node.Stub.read(node_b, read_req, @req_opts)
    assert read_reply.response == :OK
    assert read_reply.message == message

    confirm_req = ConfirmRequest.new(confirmation: :ACCEPT, id: read_reply.id)
    confirm_reply = Node.Stub.confirm(node_b, confirm_req, @req_opts)
    assert confirm_reply.response == :OK

    Process.sleep(1_000)

    context[:channels] |> Enum.each(fn node ->
      read_reply = Node.Stub.read(node, read_req, @req_opts)
      IO.inspect read_reply
      assert read_reply.response == :EMPTY
    end)
  end
end
