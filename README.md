# K3PO Nukleus Extension

[![Build Status][build-status-image]][build-status]

[build-status-image]: https://travis-ci.org/reaktivity/k3po-nukleus-ext.java.svg?branch=develop
[build-status]: https://travis-ci.org/reaktivity/k3po-nukleus-ext.java

## Nukleus `streams` Transport
Flow control with `WINDOW` update frames are managed inside the transport.

Requires external configuration of directory where streams are discovered.

```
# accept new unidirectional (simplex) streams at "receiver" from source "sender"
# for route reference ${routeRef} (required)
# with specified authorization (optional, default zeros, exact match required)
# set initial window size to 8192 (required)
# with padding size to 10 (optional)
# on explicit partition "part0" (optional: default to any partition)
# with explicit correlation ${correlationId} (optional: default to any correlation)
#
# note: throttle (default "stream") is not relevant on accept for "simplex" transmission (not writing)

property routeRef ${nukleus:newRouteRef()}
property correlationId ${nukleus:newCorrelationId()}

accept "nukleus://receiver/streams/sender"
       option nukleus:route ${routeRef}
       option nukleus:authorization 0x0001_000000_0000c1L
       option nukleus:window 8192
       option nukleus:padding 10
       option nukleus:partition "part0"
       option nukleus:correlation ${correlationId}

accepted

# receive BEGIN w/ extension
read nukleus:begin.ext [0x...]

# send BEGIN w/ extension
write nukleus:begin.ext [0x...]

connected

# expect data on explicit partition (optional)
read option nukleus:partition "part1"

# receive DATA w/ extension
read nukleus:data.ext [0x...]
read [0x...]

# send RESET
read abort

# receive ABORT
read aborted

# receive END w/ extension
read nukleus:end.ext [0x...]
read closed
```

```
# connect new unidirectional stream at "receiver" from source "sender"
# for route reference ${routeRef} (required)
# setting authorization bits on Begin (optional: default all zeros)
# on explicit partition "part0" (optional: default to any partition)
# with explicit correlation ${correlationId} (optional: default to any correlation)
# with throttle "none" (allows negative testing of flow control)


property routeRef ${nukleus:newRouteRef()}
property correlationId ${nukleus:newCorrelationId()}

connect "nukleus://receiver/streams/sender"
        option nukleus:route ${routeRef}
        option nukleus:partition "part0"
        option nukleus:correlation ${correlationId}
        option nukleus:throttle "none"
        option nukleus:authorization 0x0001_000000_0000c1L

connected

# switch writes to explicit partition (optional)
write option nukleus:partition "part1"

# send DATA w/ extension
write nukleus:data.ext [0x...]
write [0x...]

# receive RESET
write aborted

# send ABORT
write abort

# END w/ extension
write nukleus:end.ext [0x...]
write close
```

```
# accept new bidirectional streams at "receiver" from source "sender"
# for route reference ${routeRef} (required)
# with specified authorization (optional, default zeros, exact match required)
# with initial window size to 8192 (required)
# with padding size to 10 (optional)
# with throttle "none" (optional: default "stream", or "message" for per-message acknowledgment)
# with "duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "duplex" transmission

property routeRef ${nukleus:newRouteRef()}

accept "nukleus://receiver/streams/sender"
       option nukleus:route ${routeRef}
       option nukleus:window 8192
       option nukleus:padding 10
       option nukleus:throttle "none"
       option nukleus:transmission "duplex"
       option nukleus:authorization 0x0001_000000_0000c1L

accepted

# receive BEGIN w/ extension
read nukleus:begin.ext [0x...]

# send BEGIN w/ extension
write nukleus:begin.ext [0x...]

connected

# expect data on explicit partition (optional)
read option nukleus:partition "part1"

# receive DATA w/ extension
read nukleus:data.ext [0x...]
read [0x...]

# send RESET
read abort

# receive ABORT
read aborted

# receive END w/ extension
read nukleus:end.ext [0x...]
read closed

# switch writes to explicit partition (optional)
write option nukleus:partition "part1"

# send DATA w/ extension
write nukleus:data.ext [0x...]
write [0x...]

# receive RESET
write aborted

# send ABORT
write abort

# END w/ extension
write nukleus:end.ext [0x...]
write close
```

```
# connect new bidirectional stream at "receiver" from source "sender"
# for route reference ${routeRef} (required)
# setting authorization bits on Begin (optional: default all zeros)
# with initial window size to 8192 (required)
# with padding size to 10 (optional)
# with throttle "none" (optional: default "stream", or "message" for per-message acknowledgment)
# with "duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "duplex" transmission

property routeRef ${nukleus:newRouteRef()}

connect "nukleus://receiver/streams/sender"
        option nukleus:route ${routeRef}
        option nukleus:window 8192
        option nukleus:padding 10
        option nukleus:throttle "none"
        option nukleus:transmission "duplex"
        option nukleus:authorization 0x0001_000000_0000c1L

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

# receive RESET
write aborted

# send ABORT
write abort

# END w/ extension
write nukleus:end.ext [0x...]
write close

# expect data on explicit partition (optional)
read option nukleus:partition "part1"

# receive DATA w/ extension
read nukleus:data.ext [0x...]
read [0x...]

# send RESET
read abort

# receive ABORT
read aborted

# receive END w/ extension
read nukleus:end.ext [0x...]
read closed
```

```
# accept new half-duplex, bidirectional streams at "receiver" from source "sender"
# for route reference ${routeRef} (required)
# with specified authorization (optional, default zeros, exact match required)
# with initial window size to 8192 (required)
# with throttle "none" (optional: default "stream", or "message" for per-message acknowledgment)
# with "half-duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "half-duplex" transmission

property routeRef ${nukleus:newRouteRef()}

accept "nukleus://receiver/streams/sender"
       option nukleus:route ${routeRef}
       option nukleus:window 8192
       option nukleus:throttle "none"
       option nukleus:transmission "half-duplex"
       option nukleus:authorization 0x0001_000000_0000c1L

accepted

# send BEGIN w/ extension
write nukleus:begin.ext [0x...]

connected

# switch writes to explicit partition (optional)
write option nukleus:partition "part1"

# send DATA w/ extension
write nukleus:data.ext [0x...]
write [0x...]

# receive RESET
write aborted

# send ABORT
write abort

# must end write stream before reading for half-duplex
# END w/ extension
write nukleus:end.ext [0x...]
write close

# receive BEGIN w/ extension
read nukleus:begin.ext [0x...]

# expect data on explicit partition (optional)
read option nukleus:partition "part1"

# receive DATA w/ extension
read nukleus:data.ext [0x...]
read [0x...]

# send RESET
read abort

# receive ABORT
read aborted

# receive END w/ extension
read nukleus:end.ext [0x...]
read closed

```

```
# connect new half-duplex, bidirectional stream at "receiver" from source "sender"
# for route reference ${routeRef} (required)
# setting authorization bits on Begin (optional: default all zeros)
# with initial window size to 8192 (required)
# with throttle "none" (optional: default "stream", or "message" for per-message acknowledgment)
# with "half-duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "half-duplex" transmission

property routeRef ${nukleus:newRouteRef()}

connect "nukleus://receiver/streams/sender"
        option nukleus:route ${routeRef}
        option nukleus:window 8192
        option nukleus:throttle "none"
        option nukleus:transmission "half-duplex"
        option nukleus:authorization 0x0001_000000_0000c1L

# send BEGIN w/ extension
write nukleus:begin.ext [0x...]

connected

# switch writes to explicit partition (optional)
write option nukleus:partition "part1"

# send DATA w/ extension
write [0x...]
write nukleus:data.ext [0x...]

# receive RESET
write aborted

# send ABORT
write abort

# must end write stream before reading for half-duplex
# END w/ extension
write nukleus:end.ext [0x...]
write close

# receive BEGIN w/ extension
read nukleus:begin.ext [0x...]

# expect data on explicit partition (optional)
read option nukleus:partition "part1"

# receive DATA w/ extension
read nukleus:data.ext [0x...]
read [0x...]

# send RESET
read abort

# receive ABORT
read aborted

# receive END w/ extension
read nukleus:end.ext [0x...]
read closed
```


NOTE:
The "nukleus:window" option specifies the number of bytes that will be advertised in the first Window frame sent back in response to a Begin
The "nukleus:throttle" option determines how and whether writes will respect Window frames received from the other end. When set to "stream" or "message", window frames are honored, when set to "none" they are not.
