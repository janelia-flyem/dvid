// +build clustered

/*
	This file contains code for handling a Repo, the basic unit of versioning in DVID,
	and Repos, a collection of Repo.  A Repo consists of a DAG where nodes can be
	optionally locked.  In the case of multiple DVID servers accessing a shared storage engine,
	a Repo must be easily synced across distributed systems, each of which will be using the
	Repo to determine keys for the shared storage engine.

	For a clustered, non-cloud (i.e., run locally as opposed to at Google or Amazon) group of
	DVID servers, we can use Consul, etcd, or some other in-memory distributed kv store.
*/

package datastore
