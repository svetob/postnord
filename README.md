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

Publish and read a message via HTTP:
```
TODO
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

- run excellently on all modern machines with default configuration
- prefer simplicity over features
- an intuitive, easy-to-use API
- never lose data
- abstract internals away from user
- stable and reliable at any scale
