# postnord

postnord is an eventually consistent message broker.

## use cases

When you want:

- high throughput
- guaranteed durability
- fault tolerance
- easy of use
- ease of maintenance
- AP of CAP
- linear scaleability outwards
- at-least-once delivery guarantees

When you don't need:

- minimum latency
- order guarantees

## design goals

postnord should:

- run excellently on most machines with default configuration
- have an intuitive, easy-to-use API
- never lose data
- abstract internals away from the user
- run well at any scale
- be stable and reliable at any scale
- have only basic and absolutely required features

## build and test

```
mix deps.get
mix postnord.indextest
```

This will write some test data to `./data/` and measure write performance.

Tweak indextest settings to experiment with performance of e.g. small vs big
messages, or few vs many writers. See `mix help postnord.indextest`
