/*
	Package server configures and launches http/rpc server and storage engines specific
	to the type of DVID platform: local (e.g., running on MacBook Pro), clustered, or
	using cloud-based services like Google Cloud.

	Datatypes should use one of the three tiers of storage (MetaData, SmallData, BigData)
	that provide a layer of storage semantics (latency, value size, etc) on top of
	underlying storage engines.

	The DVID web client is also managed from this package.	For a DVID web console, see the
	repo:

	https://github.com/janelia-flyem/dvid-console

	The goal of a DVID web console is to provide a GUI for monitoring and performing
	a subset of operations in a nicely formatted view.

	DVID command line interaction occurs via the rpc interface to a running server.
	Please see the main DVID documentation:

	http://godoc.org/github.com/janelia-flyem/dvid
*/
package server
