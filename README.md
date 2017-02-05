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

When you don't need:

- minimum latency
- absolute maximum throughput per node
- loads of features

## design goals

postnord should:

- run excellently on most machines with default configuration
- have an intuitive, easy-to-use API
- never lose data
- abstract internals away from the user
- run well at any scale
- be stable and reliable at any scale
- have only basic and absolutely required features
