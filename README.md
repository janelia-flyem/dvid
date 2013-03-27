DVID: Distributed, Versioned Image Datastore
====

*Status: In development, not ready for use.*

DVID is a distributed, versioned image datastore that uses leveldb for data storage and a Go language layer that provides http and command-line access.

Documentation is [available here](http://godoc.org/github.com/janelia-flyem/dvid).

## Build Process

DVID uses the [buildem system](http://https://github.com/janelia-flyem/buildem#readme) to automatically
download and build leveldb, Go language support, and all required Go packages.  This can be done
manually by:

1. Build shared leveldb libraries from [Google's repo](https://code.google.com/p/leveldb/).
2. Add the following Go packages using "go get":

    go get code.google.com/p/snappy-go/snappy

    go get bitbucket.org/tebeka/nrsc

    go get code.google.com/p/go-uuid/uuid

    go get code.google.com/p/go.net/websocket
    
    go get github.com/jmhodges/levigo
    
