# Reaktivity K3PO Extensions

[![Build Status][build-status-image]][build-status]

[build-status-image]: https://travis-ci.org/reaktivity/nukleus-k3po-ext.java.svg?branch=develop
[build-status]: https://travis-ci.org/reaktivity/nukleus-k3po-ext.java

## Reaktivity `streams` Transport
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
accept reaktivity://streams/receiver
       option reaktivity:route ${routedRef}
       option reaktivity:source "sender"
       option reaktivity:window 8192
       option reaktivity:partition "part0"
       option reaktivity:correlation ${correlationId}

# receive BEGIN w/ extension
accepted
read reaktivity:extension [0x...]

# correlated streams
read reaktivity:correlation ([0..8]:captured)

# expect data on explicit partition (optional)
read option reaktivity:partition "part1"

# receive DATA w/ extension
read [0x...]
read reaktivity:extension [0x...]
read flush

# send RESET
abort

# receive END w/ extension
read closed
read reaktivity:extension [0x...]
read flush
```

```
# connect new unidirectional stream at "receiver"
# for routed reference ${routedRef} (required)
# from source "sender" (required)
# on explicit partition "part0" (optional: default to any partition)
# with explicit correlation ${correlationId} (optional: default to any correlation)
# with throttle "off" (allows negative testing of flow control)
connect reaktivity://streams/receiver
        option reaktivity:route ${routedRef}
        option reaktivity:source "sender"
        option reaktivity:partition "part0"
        option reaktivity:correlation ${correlationId}
        option reaktivity:throttle "off"

# send BEGIN w/ extension
write reaktivity:extension [0x...]

# switch writes to explicit partition (optional)
write option reaktivity:partition "part1"

# send DATA w/ extension
write [0x...]
write reaktivity:extension [0x...]
write flush

# flow control
read reaktivity:window ([0..4]:update)

# receive RESET
aborted

# END w/ extension
write close
write reaktivity:extension [0x...]
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
accept reaktivity://streams/receiver
       option reaktivity:route ${routedRef}
       option reaktivity:source "sender"
       option reaktivity:window 8192
       option reaktivity:throttle "off"
       option reaktivity:transmission "duplex"

# receive BEGIN w/ extension
accepted
read reaktivity:extension [0x...]

# correlated streams
read reaktivity:correlation ([0..8]:captured)

# expect data on explicit partition (optional)
read option reaktivity:partition "part1"

# receive DATA w/ extension
read [0x...]
read reaktivity:extension [0x...]
read flush

# send RESET
abort

# receive END w/ extension
read closed
read reaktivity:extension [0x...]
read flush

# send BEGIN w/ extension
write extension [0x...]

# switch writes to explicit partition (optional)
write option reaktivity:partition "part1"

# send DATA w/ extension
write [0x...]
write reaktivity:extension [0x...]
write flush

# flow control
read reaktivity:window ([0..4]:update)

# receive RESET
aborted

# END w/ extension
write close
write reaktivity:extension [0x...]
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
connect reaktivity://streams/receiver
        option reaktivity:route ${routedRef}
        option reaktivity:source "sender"
        option reaktivity:window 8192
        option reaktivity:throttle "off"
        option reaktivity:transmission "duplex"

connected

# send BEGIN w/ extension
write reaktivity:extension [0x...]

# switch writes to explicit partition (optional)
write option reaktivity:partition "part1"

# send DATA w/ extension
write [0x...]
write reaktivity:extension [0x...]
write flush

# flow control
read reaktivity:window ([0..4]:update)

# receive RESET
aborted

# END w/ extension
write close
write reaktivity:extension [0x...]
write flush

# receive BEGIN w/ extension
read reaktivity:extension [0x...]

# correlated streams
read reaktivity:correlation ([0..8]:captured)

# expect data on explicit partition (optional)
read option reaktivity:partition "part1"

# receive DATA w/ extension
read [0x...]
read reaktivity:extension [0x...]
read flush

# send RESET
abort

# receive END w/ extension
read closed
read reaktivity:extension [0x...]
read flush
```
