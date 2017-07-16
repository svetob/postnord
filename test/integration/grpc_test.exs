defmodule Postnord.Test.Integration.GRPC do
  use ExUnit.Case, async: false

  @moduledoc """
  Test gRPC server endpoints, client and functionality.
  """

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
    write_request = Postnord.GRPC.WriteRequest.new(message: message)
    write_reply = Postnord.GRPC.Node.Stub.write(chan, write_request, timeout: 100_000)

    assert write_reply.response == :OK

    # Read message
    read_request = Postnord.GRPC.ReadRequest.new()
    read_reply = Postnord.GRPC.Node.Stub.read(chan, read_request, timeout: 100_000)

    assert read_reply.response == :OK
    assert read_reply.message == message

    # Confirm message
    confirm_request = Postnord.GRPC.ConfirmRequest.new(confirmation: :ACCEPT, id: read_reply.id)
    confirm_reply = Postnord.GRPC.Node.Stub.confirm(chan, confirm_request, timeout: 100_000)

    assert confirm_reply.response == :OK

    # Ensure message is confirmed
    read_reply = Postnord.GRPC.Node.Stub.read(chan, read_request, timeout: 100_000)

    assert read_reply.response == :EMPTY
  end

  test "can write, read, confirm from different connections" do
    {:ok, chan_write} = GRPC.Stub.connect(uri())
    {:ok, chan_read} = GRPC.Stub.connect(uri())
    {:ok, chan_confirm} = GRPC.Stub.connect(uri())

    message = "Hello multi-gRPC!"

    # Write a message
    write_request = Postnord.GRPC.WriteRequest.new(message: message)
    write_reply = Postnord.GRPC.Node.Stub.write(chan_write, write_request, timeout: 100_000)

    assert write_reply.response == :OK

    # Read message
    read_request = Postnord.GRPC.ReadRequest.new()
    read_reply = Postnord.GRPC.Node.Stub.read(chan_read, read_request, timeout: 100_000)

    assert read_reply.response == :OK
    assert read_reply.message == message

    # Confirm message
    confirm_request = Postnord.GRPC.ConfirmRequest.new(confirmation: :ACCEPT, id: read_reply.id)
    confirm_reply = Postnord.GRPC.Node.Stub.confirm(chan_confirm, confirm_request, timeout: 100_000)

    assert confirm_reply.response == :OK

    # Ensure message is confirmed
    read_reply = Postnord.GRPC.Node.Stub.read(chan_read, read_request, timeout: 100_000)

    assert read_reply.response == :EMPTY
  end

  def uri do
    grpc_port = Application.get_env(:postnord, :grpc_port)
    "localhost:#{grpc_port}"
  end
end
