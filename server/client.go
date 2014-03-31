/*
	This file handles client communication to a remote DVID server.
*/

package server

import (
	"fmt"
	"net/rpc"
	"os"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
)

// Client provides RPC access to a DVID server.
type Client struct {
	rpcAddress string
	version    string
	client     *rpc.Client
}

// NewClient returns an RPC client to the given address.
func NewClient(rpcAddress string) *Client {
	client, err := rpc.DialHTTP("tcp", rpcAddress)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Did not find DVID server for RPC at %s  [%s]\n",
			rpcAddress, err.Error())
		client = nil // Close connection if any error and try serverless mode.
	}
	return &Client{
		rpcAddress: rpcAddress,
		client:     client,
	}
}

// Send transmits an RPC command if a server is available.
func (c *Client) Send(request datastore.Request) error {
	var reply datastore.Response
	if c.client != nil {
		err := c.client.Call("RPCConnection.Do", request, &reply)
		if err != nil {
			if dvid.Mode == dvid.Debug {
				return fmt.Errorf("RPC error for '%s': %s", request.Command, err.Error())
			} else {
				return fmt.Errorf("RPC error: %s", err.Error())
			}
		}
	} else {
		reply.Output = []byte(fmt.Sprintf("No DVID server is available: %s\n", request.Command))
	}
	return reply.Write(os.Stdout)
}
