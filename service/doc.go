/*
Package service provides an interface for registering services.

Main service.go file has an interface that all services should adopt
and provides functionality for registering a service with DVID
and providing meta information regarding the status of the service.

ASSUMPTION: For now, assume that service are appliable over a node
in a dataset.  In the future, it might be useful for services
to be applied over all datasets served by the server.
*/
package service
