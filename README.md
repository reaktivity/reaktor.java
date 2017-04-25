# Nukleus K3PO Extensions

[![Build Status][build-status-image]][build-status]

[build-status-image]: https://travis-ci.org/reaktivity/nukleus-k3po-ext.java.svg?branch=develop
[build-status]: https://travis-ci.org/reaktivity/nukleus-k3po-ext.java

## Nukleus `streams` Transport
Flow control with `WINDOW` update frames are managed inside the transport.

Requires external configuration of directory where streams are discovered.

```
# accept new unidirectional (simplex) streams at "receiver" from source "sender"
# for routed reference ${routedRef} (required)
# set initial window size to 8192 (required)
# on explicit partition "part0" (optional: default to any partition)
# with explicit correlation ${correlationId} (optional: default to any correlation)
#
# note: throttle (default "on") is not relevant on accept for "simplex" transmission (not writing)
accept "nukleus://receiver/streams/sender"
       option nukleus:route ${routedRef}
       option nukleus:window 8192
       option nukleus:partition "part0"
       option nukleus:correlation ${correlationId}

accepted

# receive BEGIN w/ extension
read nukleus:begin.ext [0x...]

# send BEGIN w/ extension
write nukleus:begin.ext [0x...]

connected

# correlated streams
read nukleus:correlation ([0..8]:captured)

# expect data on explicit partition (optional)
read option nukleus:partition "part1"

# receive DATA w/ extension
read nukleus:data.ext [0x...]
read [0x...]

# send RESET
abort

# receive END w/ extension
read nukleus:end.ext [0x...]
read closed
```

```
# connect new unidirectional stream at "receiver" from source "sender"
# for routed reference ${routedRef} (required)
# on explicit partition "part0" (optional: default to any partition)
# with explicit correlation ${correlationId} (optional: default to any correlation)
# with throttle "off" (allows negative testing of flow control)
connect "nukleus://receiver/streams/sender"
        option nukleus:route ${routedRef}
        option nukleus:partition "part0"
        option nukleus:correlation ${correlationId}
        option nukleus:throttle "off"

connected

# switch writes to explicit partition (optional)
write option nukleus:partition "part1"

# send DATA w/ extension
write nukleus:data.ext [0x...]
write [0x...]

# flow control
read nukleus:window ([0..4]:update)

# receive RESET
aborted

# END w/ extension
write nukleus:end.ext [0x...]
write close
```

```
# accept new bidirectional streams at "receiver" from source "sender"
# for routed reference ${routedRef} (required)
# with initial window size to 8192 (required)
# with throttle "off" (optional: default "on")
# with "duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "duplex" transmission
accept "nukleus://receiver/streams/sender"
       option nukleus:route ${routedRef}
       option nukleus:window 8192
       option nukleus:throttle "off"
       option nukleus:transmission "duplex"

accepted

# receive BEGIN w/ extension
read nukleus:begin.ext [0x...]

# send BEGIN w/ extension
write nukleus:begin.ext [0x...]

connected

# correlated streams
read nukleus:correlation ([0..8]:captured)

# expect data on explicit partition (optional)
read option nukleus:partition "part1"

# receive DATA w/ extension
read nukleus:data.ext [0x...]
read [0x...]

# send RESET
abort

# receive END w/ extension
read nukleus:end.ext [0x...]
read closed

# switch writes to explicit partition (optional)
write option nukleus:partition "part1"

# send DATA w/ extension
write nukleus:data.ext [0x...]
write [0x...]

# flow control
read nukleus:window ([0..4]:update)

# receive RESET
aborted

# END w/ extension
write nukleus:end.ext [0x...]
write close
```

```
# connect new bidirectional stream at "receiver" from source "sender"
# for routed reference ${routedRef} (required)
# with initial window size to 8192 (required)
# with throttle "off" (optional: default "on")
# with "duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "duplex" transmission
connect "nukleus://receiver/streams/sender"
        option nukleus:route ${routedRef}
        option nukleus:window 8192
        option nukleus:throttle "off"
        option nukleus:transmission "duplex"

# send BEGIN w/ extension
write nukleus:begin.ext [0x...]

# receive BEGIN w/ extension
read nukleus:begin.ext [0x...]

connected

# switch writes to explicit partition (optional)
write option nukleus:partition "part1"

# send DATA w/ extension
write [0x...]
write nukleus:data.ext [0x...]

# flow control
read nukleus:window ([0..4]:update)

# receive RESET
aborted

# END w/ extension
write nukleus:end.ext [0x...]
write close

# correlated streams
read nukleus:correlation ([0..8]:captured)

# expect data on explicit partition (optional)
read option nukleus:partition "part1"

# receive DATA w/ extension
read nukleus:data.ext [0x...]
read [0x...]

# send RESET
abort

# receive END w/ extension
read nukleus:end.ext [0x...]
read closed
```
