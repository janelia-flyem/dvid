/*
Package service provides an interface for registering services.

Main service.go file has an interface that all services should adopt
and provides functionality for registering a service with DVID
and providing meta information regarding the status of the service.

ASSUMPTION: For now, assume that service are appliable over a node
in a dataset.  In the future, it might be useful for services
to be applied over all datasets served by the server.

1. Service Developers

A service can be called as long as it defines a JSON contract for how
to interact with it and implements an interface which can invoke it
(such as executables, internal go routines, http rest services, etc).

helloworld.go provides an example of how to create a service that can
be registered with DVID and can be called by DVID's REST API.  Every
service created should be callable through a RunService(string) method,
which implements the mandatory ServiceExecutor interface.
Currently, the service.go file provides a simple type called
ServiceLocalExe for a service that runs using a locally installed
executable.  In this helloworld example, the service is implemented
as a go routine.  To create a service type, the service must also specify
the contract (or interface) for calling the service.

Each service will be called with the JSON input that satisifies its
interface.  If the service is in an external program and requires information
beyond the json input, it is possible for a developer to make his/her custom
implementation of the ServiceExecutor interface.  It is expected, for the most
part, that transfer of large binary data will happen by accesing DVID
through links.

A service will receive meta data from the DVID server (internal API) that is
not relevant to the initial service caller.  The server will send data
like the current uuid and server-path (see the helloworld example).
In particular, an access-key and callback path are specified.  It is expected,
but not mandated, that a service write back the status of the service
job and results (if necessary) to the callback.  These actions require authentication
with a randomly-generated access key provided by DVID.  Here are examples
on how to update the service status and result using Curl:

// add a result for the given service run
curl -u <access-key>: -X PUT http://<server-path>/<callback>/result -F data=@result.json

// add a status result for the given service run
curl -u <access-key>: -X PUT -H "Content-Type: application/json" http://<server-path>/<callback> -d '{"status": "finished"}'

2.  Service Clients

Currently, services are implemented as datatypes in DVID.  This means that a service
type, like helloworld, needs to be instantiated for each node and for each dataset
where a service must be run.  Another consequence is that, in theory, a service could
be available and created at one time, but be unavailable later.  In the future,
we will have a more general service discovery mechanism to prevent these problems.

For example, to create a new service type and associate it with a given node e0:

dvid dataset e0 new helloworld hello

Running the service involves POSTing to hello.  For example, to call hello using Curl:

curl -X POST -H "Content-Type: application/json" http://<server-path>/api/node/e0/hello -d '{"message": "World!", "interface-version" : "0.1"}'

POSTing to this service will invoke the service and create a new service ID.  This service
ID can be used to query the status of the service and to check for results (if there
are any).  The POST command will return a callback, for example:

{"callback" : "/api/node/e0/hello/47"}

This means that the new service call has an ID of '47'.

The status of the service is returned as a JSON at:

http://<server-path>/api/node/e0/hell0/47

The result of service is returned as an "octet-stream" type for arbitrary binary data:

http://<server-path>/api/node/e0/hello/47/result
*/
package service
