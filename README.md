# Nukleus K3PO Extensions

[![Build Status][build-status-image]][build-status]

[build-status-image]: https://travis-ci.org/reaktivity/nukleus-k3po-ext.java.svg?branch=develop
[build-status]: https://travis-ci.org/reaktivity/nukleus-k3po-ext.java

## Nukleus `stream` Transport
Flow control with `WINDOW` update frames are managed inside the transport.

Requires external configuration of directory where streams are discovered.

```
# expect new streams from "source" to "nukleus"
# on explicit partition "part0" (optional: default to any partition)
# set initial window size to 8192
accept nukleus://stream/nukleus/source
       option window 8192
       option partition "part0"

# receive BEGIN w/ extension
accepted
read extension [0x...]

# expect data on explicit partition (optional)
read option partition "part1"

# receive DATA w/ extension
read [0x...]
read extension [0x...]
read flush

# send RESET
abort

# receive END w/ extension
closed extension [0x...]
```

```
# send BEGIN w/ extension for new stream from "nukleus" to "target"
# on explicit partition "part0" (optional: default to no partition)
# and initial window size to 2048 (optional: default 0 before first WINDOW frame)
connect nukleus://stream/nukleus/target
        option partition "part0"
        option window 2048
write extension [0x...]

# switch writes to explicit partition (optional)
write option partition "part1"

# send DATA w/ extension
write [0x...]
write extension [0x...]
write flush

# receive RESET
aborted

# END w/ extension
close extension [0x...]
```
