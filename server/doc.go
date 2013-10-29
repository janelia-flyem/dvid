/*
Package server provides web and rpc interfaces to DVID operations.  It is also
the likely package to manage polyglot persistence, i.e., given a data type and
UUID, direct it to the appropriate storage engine (datastore service).

For a DVID web console, see the repo:

https://github.com/janelia-flyem/dvid-webclient

The goal of a DVID web console is to provide a GUI for monitoring and performing
a subset of operations in a nicely formatted view.

DVID command line interaction occurs via the rpc interface to a running server.
Please see the main DVID documentation:

http://godoc.org/github.com/janelia-flyem/dvid
*/
package server
