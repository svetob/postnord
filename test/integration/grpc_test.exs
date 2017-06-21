defmodule Postnord.Test.EndToEnd do
  use ExUnit.Case, async: false

  test "can connect to gRPC server" do
    {:ok, _chan} = GRPC.Stub.connect(uri())
  end

  test "can write, read and confirm a message" do
    {:ok, chan} = GRPC.Stub.connect(uri())

    message = "Hello gRPC!"

    # Write a message
    write_request = Postnord.GRPC.WriteRequest.new(message: message)
    write_reply = Postnord.GRPC.Node.Stub.write(chan, write_request)

    assert write_reply.response == :OK

    # Read message
    read_request = Postnord.GRPC.ReadRequest.new()
    read_reply = Postnord.GRPC.Node.Stub.read(chan, read_request)

    assert read_reply.response == :OK
    assert read_reply.message == message

    # Confirm message
    confirm_request = Postnord.GRPC.ConfirmRequest.new(confirmation: :ACCEPT, id: read_reply.id)
    confirm_reply = Postnord.GRPC.Node.Stub.confirm(chan, confirm_request)

    assert confirm_reply.response == :OK

    # Ensure message is confirmed
    read_reply = Postnord.GRPC.Node.Stub.read(chan, read_request)

    assert read_reply.response == :EMPTY
  end

  def uri do
    grpc_port = Application.get_env(:postnord, :grpc_port)
    "localhost:#{grpc_port}"
  end
end
