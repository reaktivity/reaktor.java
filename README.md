# Nukleus K3PO Extensions

[![Build Status][build-status-image]][build-status]

[build-status-image]: https://travis-ci.org/reaktivity/nukleus-k3po-ext.java.svg?branch=develop
[build-status]: https://travis-ci.org/reaktivity/nukleus-k3po-ext.java

## Nukleus `stream` Transport
Flow control with `WINDOW` update frames are managed inside the transport.

Requires external configuration of directory where streams are discovered.

```
# accept new unidirectional (simplex) streams at "receiver"
# for routed reference ${routedRef} (required)
# from source "sender" (required)
# set initial window size to 8192 (required)
# on explicit partition "part0" (optional: default to any partition)
# with explicit correlation ${correlationId} (optional: default to any correlation)
#
# note: throttle (default "on") is not relevant on accept for "simplex" transmission (not writing)
accept nukleus://streams/receiver
       option route ${routedRef}
       option source "sender"
       option window 8192
       option partition "part0"
       option correlation ${correlationId}

# receive BEGIN w/ extension
accepted
read extension [0x...]

# correlated streams
read correlation ([0..8]:captured)

# expect data on explicit partition (optional)
read option partition "part1"

# receive DATA w/ extension
read [0x...]
read extension [0x...]
read flush

# send RESET
abort

# receive END w/ extension
read closed
read extension [0x...]
read flush
```

```
# connect new unidirectional stream at "receiver"
# for routed reference ${routedRef} (required)
# from source "sender" (required)
# on explicit partition "part0" (optional: default to any partition)
# with explicit correlation ${correlationId} (optional: default to any correlation)
# with throttle "off" (allows negative testing of flow control)
connect nukleus://streams/receiver
        option route ${routedRef}
        option source "sender"
        option partition "part0"
        option correlation ${correlationId}
        option throttle "off"

# send BEGIN w/ extension
write extension [0x...]

# switch writes to explicit partition (optional)
write option partition "part1"

# send DATA w/ extension
write [0x...]
write extension [0x...]
write flush

# flow control
read window ([0..4]:update)

# receive RESET
aborted

# END w/ extension
write close
write extension [0x...]
write flush
```

```
# accept new bidirectional streams at "receiver"
# for routed reference ${routedRef} (required)
# from source "sender" (required)
# with initial window size to 8192 (required)
# with throttle "off" (optional: default "on")
# with "duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "duplex" transmission
accept nukleus://streams/receiver
       option route ${routedRef}
       option source "sender"
       option window 8192
       option throttle "off"
       option transmission "duplex"

# receive BEGIN w/ extension
accepted
read extension [0x...]

# correlated streams
read correlation ([0..8]:captured)

# expect data on explicit partition (optional)
read option partition "part1"

# receive DATA w/ extension
read [0x...]
read extension [0x...]
read flush

# send RESET
abort

# receive END w/ extension
read closed
read extension [0x...]
read flush

# send BEGIN w/ extension
write extension [0x...]

# switch writes to explicit partition (optional)
write option partition "part1"

# send DATA w/ extension
write [0x...]
write extension [0x...]
write flush

# flow control
read window ([0..4]:update)

# receive RESET
aborted

# END w/ extension
write close
write extension [0x...]
write flush
```

```
# connect new bidirectional stream at "receiver"
# for routed reference ${routedRef} (required)
# from source "sender" (required)
# with initial window size to 8192 (required)
# with throttle "off" (optional: default "on")
# with "duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "duplex" transmission
connect nukleus://streams/receiver
        option route ${routedRef}
        option source "sender"
        option window 8192
        option throttle "off"
        option transmission "duplex"

connected

# send BEGIN w/ extension
write extension [0x...]

# switch writes to explicit partition (optional)
write option partition "part1"

# send DATA w/ extension
write [0x...]
write extension [0x...]
write flush

# flow control
read window ([0..4]:update)

# receive RESET
aborted

# END w/ extension
write close
write extension [0x...]
write flush

# receive BEGIN w/ extension
read extension [0x...]

# correlated streams
read correlation ([0..8]:captured)

# expect data on explicit partition (optional)
read option partition "part1"

# receive DATA w/ extension
read [0x...]
read extension [0x...]
read flush

# send RESET
abort

# receive END w/ extension
read closed
read extension [0x...]
read flush
```
