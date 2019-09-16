# K3PO Nukleus Extension

[![Build Status][build-status-image]][build-status]
[![Code Coverage][code-coverage-image]][code-coverage]

#### Build
```bash
./mvnw clean install
```
#### Build on Windows
```bash
mvnw.cmd clean install
```

[build-status-image]: https://travis-ci.com/reaktivity/k3po-nukleus-ext.java.svg?branch=develop
[build-status]: https://travis-ci.com/reaktivity/k3po-nukleus-ext.java
[code-coverage-image]: https://codecov.io/gh/reaktivity/k3po-nukleus-ext.java/branch/develop/graph/badge.svg
[code-coverage]: https://codecov.io/gh/reaktivity/k3po-nukleus-ext.java

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
# using network byte order (big-endian) for shorts, int's and longs (optional: default to native)
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
       option nukleus:byteorder "network"

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

# receive DATA w/ extension and null payload
read nukleus:data.ext [0x...]
read nukleus:data.null

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
# using network byte order (big-endian) for shorts, int's and longs (optional: default to native)
# with throttle "none" (allows negative testing of flow control)


property routeRef ${nukleus:newRouteRef()}
property correlationId ${nukleus:newCorrelationId()}

connect "nukleus://receiver/streams/sender"
        option nukleus:route ${routeRef}
        option nukleus:authorization 0x0001_000000_0000c1L
        option nukleus:partition "part0"
        option nukleus:correlation ${correlationId}
        option nukleus:throttle "none"

connected

# switch writes to explicit partition (optional)
write option nukleus:partition "part1"

# send DATA w/ extension
write nukleus:data.ext [0x...]
write [0x...]

# send DATA w/ extension and null payload
write nukleus:data.ext [0x...]
write flush

# send DATA w/ extension and empty payload
write nukleus:data.ext [0x...]
write nukleus:data.empty

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
# using network byte order (big-endian) for shorts, int's and longs (optional: default to native)
# with "duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "duplex" transmission

property routeRef ${nukleus:newRouteRef()}

accept "nukleus://receiver/streams/sender"
       option nukleus:route ${routeRef}
       option nukleus:authorization 0x0001_000000_0000c1L
       option nukleus:window 8192
       option nukleus:padding 10
       option nukleus:throttle "none"
       option nukleus:byteorder "native"
       option nukleus:transmission "duplex"

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

# receive DATA w/ extension and null payload
read nukleus:data.ext [0x...]
read nukleus:data.null

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

# send DATA w/ extension and null payload
write nukleus:data.ext [0x...]
write flush

# send DATA w/ extension and empty payload
write nukleus:data.ext [0x...]
write nukleus:data.empty

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
# using network byte order (big-endian) for shorts, int's and longs (optional: default to native)
# with "duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "duplex" transmission

property routeRef ${nukleus:newRouteRef()}

connect "nukleus://receiver/streams/sender"
        option nukleus:route ${routeRef}
        option nukleus:authorization 0x0001_000000_0000c1L
        option nukleus:window 8192
        option nukleus:padding 10
        option nukleus:throttle "none"
        option nukleus:byteorder "native"
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

# send DATA w/ extension and null payload
write nukleus:data.ext [0x...]
write flush

# send DATA w/ extension and empty payload
write nukleus:data.ext [0x...]
write nukleus:data.empty

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

# receive DATA w/ extension and null payload
read nukleus:data.ext [0x...]
read nukleus:data.null

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
# using network byte order (big-endian) for shorts, int's and longs (optional: default to native)
# with "half-duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "half-duplex" transmission

property routeRef ${nukleus:newRouteRef()}

accept "nukleus://receiver/streams/sender"
       option nukleus:route ${routeRef}
       option nukleus:authorization 0x0001_000000_0000c1L
       option nukleus:window 8192
       option nukleus:throttle "none"
       option nukleus:byteorder "native"
       option nukleus:transmission "half-duplex"

accepted

# send BEGIN w/ extension
write nukleus:begin.ext [0x...]

connected

# switch writes to explicit partition (optional)
write option nukleus:partition "part1"

# send DATA w/ extension
write nukleus:data.ext [0x...]
write [0x...]

# send DATA w/ extension and null payload
write nukleus:data.ext [0x...]
write flush

# send DATA w/ extension and empty payload
write nukleus:data.ext [0x...]
write nukleus:data.empty

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

# receive DATA w/ extension and null payload
read nukleus:data.ext [0x...]
read nukleus:data.null

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
# using network byte order (big-endian) for shorts, int's and longs (optional: default to native)
# with "half-duplex" transmission for bidirectional (optional: default "simplex")
#
# note: partition, correlationId not supported for "half-duplex" transmission

property routeRef ${nukleus:newRouteRef()}

connect "nukleus://receiver/streams/sender"
        option nukleus:route ${routeRef}
        option nukleus:authorization 0x0001_000000_0000c1L
        option nukleus:window 8192
        option nukleus:throttle "none"
        option nukleus:byteorder "network"
        option nukleus:transmission "half-duplex"

# send BEGIN w/ extension
write nukleus:begin.ext [0x...]

connected

# switch writes to explicit partition (optional)
write option nukleus:partition "part1"

# send DATA w/ extension
write [0x...]
write nukleus:data.ext [0x...]

# send DATA w/ extension and null payload
write nukleus:data.ext [0x...]
write flush

# send DATA w/ extension and empty payload
write nukleus:data.ext [0x...]
write nukleus:data.empty

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

# receive DATA w/ extension and null payload
read nukleus:data.ext [0x...]
read nukleus:data.null

# send RESET
read abort

# receive ABORT
read aborted

# receive END w/ extension
read nukleus:end.ext [0x...]
read closed
```


NOTE:
The `nukleus:window` option specifies the number of bytes that will be advertised in the first `WINDOW` frame sent back in response to a `BEGIN`
The `nukleus:throttle` option determines how and whether writes will respect `WINDOW` frames received from the other end. When set to `"stream"` or `"message"`, window frames are honored, when set to `"none"` they are not.
The `nukleus:alignment` option with value `"message"` requires a complete message to be matched by each `read` statement, even if fragmented into multiple `DATA` frames.
