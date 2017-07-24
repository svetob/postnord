defmodule Postnord.Test.Cluster.RequestCoordination do
  require Logger
  use ExUnit.Case, async: false

  @moduledoc """
  Cluster tests which verify requests are correctly coordinated
  accross multiple nodes.
  """

  alias Postnord.GRPC.ConfirmRequest
  alias Postnord.GRPC.FlushRequest
  alias Postnord.GRPC.WriteRequest
  alias Postnord.GRPC.WriteReply
  alias Postnord.GRPC.ReadRequest
  alias Postnord.GRPC.Node

  @req_opts [timeout: 10_000_000]

  setup_all do
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

  setup context do
    on_exit fn ->
      Logger.info "Flushing cluster"
      channel = hd(context[:channels])
      reply = Node.Stub.flush(channel, FlushRequest.new())
      assert reply.success
    end
    context
  end


  test "can write to one node then read from another", context do
    [node_a, node_b, _] = context[:channels]

    message = RandomBytes.base62(1024)

    write_req = WriteRequest.new(message: message)
    %WriteReply{response: :OK} = Node.Stub.write(node_a, write_req, @req_opts)

    read_req = ReadRequest.new()
    read_reply = Node.Stub.read(node_b, read_req, @req_opts)
    assert read_reply.response == :OK
    assert read_reply.message == message
  end

  test "cannot see message from any node after queue flush", context do
    node_a = hd(context[:channels])

    message = RandomBytes.base62(1024)

    write_req = WriteRequest.new(message: message)
    write_reply = Node.Stub.write(node_a, write_req, @req_opts)
    assert write_reply.response == :OK

    flush_req = FlushRequest.new()
    flush_reply = Node.Stub.flush(node_a, flush_req, @req_opts)
    assert flush_reply.success

    Process.sleep(200)

    context[:channels] |> Enum.each(fn node ->
      read_req = ReadRequest.new()
      read_reply = Node.Stub.read(node, read_req, @req_opts)
      assert read_reply.response == :EMPTY
    end)
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
    [node_a, node_b, _] = context[:channels]

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

    Process.sleep(100)

    context[:channels] |> Enum.each(fn node ->
      read_reply = Node.Stub.read(node, read_req, @req_opts)
      assert read_reply.response == :EMPTY
    end)
  end
end
