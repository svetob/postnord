defmodule Postnord.Test.Integration.GRPC do
  alias Postnord.GRPC.ConfirmRequest
  alias Postnord.GRPC.FlushRequest
  alias Postnord.GRPC.ReadRequest
  alias Postnord.GRPC.ReplicateRequest
  alias Postnord.GRPC.TombstoneRequest
  alias Postnord.GRPC.WriteRequest
  alias Postnord.GRPC.Node.Stub
  use ExUnit.Case, async: false

  @moduledoc """
  Test gRPC server endpoints, client and functionality.
  """

  @grpc_opts [timeout: 1_000_000]

  test "can connect to gRPC server" do
    {:ok, _chan} = GRPC.Stub.connect(uri())
  end

  test "can open multiple connections to gRPC server" do
    {:ok, _chan} = GRPC.Stub.connect(uri())
    {:ok, _chan} = GRPC.Stub.connect(uri())
    {:ok, _chan} = GRPC.Stub.connect(uri())
  end

  test "can write, read and confirm a message" do
    {:ok, chan} = GRPC.Stub.connect(uri())

    message = "Hello gRPC!"

    # Write a message
    write_request = WriteRequest.new(message: message)
    write_reply = Stub.write(chan, write_request)

    assert write_reply.response == :OK

    # Read message
    read_request = ReadRequest.new()
    read_reply = Stub.read(chan, read_request, @grpc_opts)

    assert read_reply.response == :OK
    assert read_reply.message == message

    # Confirm message
    confirm_request = ConfirmRequest.new(confirmation: :ACCEPT, id: read_reply.id)
    confirm_reply = Stub.confirm(chan, confirm_request, @grpc_opts)

    assert confirm_reply.response == :OK

    # Ensure message is confirmed
    read_reply = Stub.read(chan, read_request, @grpc_opts)

    assert read_reply.response == :EMPTY
  end

  test "can write, read, confirm from different connections" do
    {:ok, chan_write} = GRPC.Stub.connect(uri())
    {:ok, chan_read} = GRPC.Stub.connect(uri())
    {:ok, chan_confirm} = GRPC.Stub.connect(uri())

    message = "Hello multi-gRPC!"

    # Write a message
    write_request = WriteRequest.new(message: message)
    write_reply = Stub.write(chan_write, write_request, @grpc_opts)

    assert write_reply.response == :OK

    # Read message
    read_request = ReadRequest.new()
    read_reply = Stub.read(chan_read, read_request, @grpc_opts)

    assert read_reply.response == :OK
    assert read_reply.message == message

    # Confirm message
    confirm_request = ConfirmRequest.new(confirmation: :ACCEPT, id: read_reply.id)
    confirm_reply = Stub.confirm(chan_confirm, confirm_request, @grpc_opts)

    assert confirm_reply.response == :OK

    # Ensure message is confirmed
    read_reply = Stub.read(chan_read, read_request, @grpc_opts)

    assert read_reply.response == :EMPTY
  end

  test "can flush queue" do
    {:ok, chan} = GRPC.Stub.connect(uri())

    message = "Flush me"

    # Write a message
    write_request = WriteRequest.new(message: message)
    write_reply = Stub.write(chan, write_request)

    assert write_reply.response == :OK

    # Flush queue
    flush_request = FlushRequest.new()
    flush_reply = Stub.flush(chan, flush_request, @grpc_opts)
    assert flush_reply.success

    # Ensure message is confirmed
    read_request = ReadRequest.new()
    read_reply = Stub.read(chan, read_request, @grpc_opts)

    assert read_reply.response == :EMPTY
  end

#  test "can replicate a message" do
#    {:ok, chan} = GRPC.Stub.connect(uri())
#
#    id = Postnord.IdGen.message_id()
#    message = "Hello multi-gRPC!"
#
#    # Replicate message
#    replicate_request = ReplicateRequest.new(id: id, message: message)
#    replicate_reply = Stub.replicate(chan, replicate_request, @grpc_opts)
#
#    assert replicate_reply.success
#
#    # Ensure message is replicated
#    read_request = ReadRequest.new()
#    read_reply = Stub.read(chan, read_request, @grpc_opts)
#
#    assert read_reply.response == :OK
#    assert read_reply.message == message
#  end

  test "can tombstone a message" do
    {:ok, chan} = GRPC.Stub.connect(uri())

    id = Postnord.IdGen.message_id
    message = "Hello multi-gRPC!"

    # Replicate message
    replicate_request = ReplicateRequest.new(id: id, message: message)
    replicate_reply = Stub.replicate(chan, replicate_request, @grpc_opts)

    assert replicate_reply.success

    # Tombstone
    tombstone_request = TombstoneRequest.new(id: id)
    tombstone_reply = Stub.tombstone(chan, tombstone_request, @grpc_opts)

    assert tombstone_reply.success

    # Ensure message is not returned on read
    read_request = ReadRequest.new()
    read_reply = Stub.read(chan, read_request, @grpc_opts)

    assert read_reply.response == :EMPTY
  end

  def uri do
    grpc_port = Application.get_env(:postnord, :grpc_port)
    "localhost:#{grpc_port}"
  end
end
