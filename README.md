# postnord [![Build Status](https://travis-ci.org/svetob/postnord.svg?branch=master)](https://travis-ci.org/svetob/postnord)

An eventually consistent message broker, written in Elixir.

## quick-start

Build and test:
```
$ mix deps.get
$ mix test
```

Launch postnord server:
```
$ mix run --no-halt -e "Postnord.main()"
```

Publish and read a message via gRPC:
```elixir
$ iex -S mix

# Open gRPC connection
{:ok, chan} = GRPC.Stub.connect("localhost:2021")

# Write a message
write_request = Postnord.GRPC.WriteRequest.new(message: "Written by GRPC!")
write_reply = Postnord.GRPC.Node.Stub.write(chan, write_request)

# Read message
read_request = Postnord.GRPC.ReadRequest.new()
read_reply = Postnord.GRPC.Node.Stub.read(chan, read_request)

# Confirm message
confirm_request = Postnord.GRPC.ConfirmRequest.new(confirmation: :ACCEPT, id: read_reply.id)
confirm_reply = Postnord.GRPC.Node.Stub.confirm(chan, confirm_request)
```

## performance test

Run a mixed read/write performance test:

```
$ mix postnord.perftest.mixed
```

Tweak test settings to experiment with performance of e.g. small vs big
messages, or few vs many writers. See `mix help postnord.perftest.mixed`

## use cases

When you want:

- high throughput
- guaranteed durability
- fault tolerance
- easy of use
- ease of maintenance
- AP
- linear scaleability
- at-least-once delivery guarantees

When you don't need:

- minimum latency
- order guarantees

## design goals

- run excellently on most machines with default configuration
- an intuitive, easy-to-use API
- never lose data
- abstract internals away from user
- stable and reliable at any scale
- prefer simplicity over features
