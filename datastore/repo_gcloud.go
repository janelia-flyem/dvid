// +build gcloud

/*
	This file contains code for handling a Repo, the basic unit of versioning in DVID,
	and Repos, a collection of Repo.  A Repo consists of a DAG where nodes can be
	optionally locked.  In the case of multiple DVID servers accessing a shared storage engine,
	a Repo must be easily synced across distributed systems, each of which will be using the
	Repo to determine keys for the shared storage engine.

	For the Google cloud implementation, we would use the memcache and datastore APIs to implement
	the MetadataStore.
*/

package datastore
