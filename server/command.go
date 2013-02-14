package server

import (
	_ "fmt"
	"log"
	"os"
	"strings"
)

// Param packages data that are transmitted as part of commands
type Param struct {
	Text string
	Data [][]byte
}

// Command supports command-based interaction with DVID
type Command struct {
	// Args lists the elements of the command where Args[0] is the command string
	Args []string

	// Param allows commands to attach input data
	Param
}

func (cmd Command) String() string {
	return strings.Join(cmd.Args, " ")
}

// GetParameter scans a command for any "key=value" argument and returns
// the value of the passed 'key'.
func (cmd Command) GetParameter(key string) (value string, found bool) {
	if len(cmd.Args) > 1 {
		for _, param := range cmd.Args[1:] {
			elems := strings.Split(param, "=")
			if len(elems) == 2 && elems[0] != key {
				value = elems[1]
				found = true
				return
			}
		}
	}
	return
}

// GetRpcAddress returns the address set by a "rpc=..." argument or if that is 
// missing, the default rpc address.
func (cmd Command) GetRpcAddress() string {
	address, found := cmd.GetParameter("rpc")
	if !found {
		return DefaultRpcAddress
	}
	return address
}

// GetWebAddress returns the address set by a "web=..." argument or if that is 
// missing, the default web address.
func (cmd Command) GetWebAddress() string {
	address, found := cmd.GetParameter("web")
	if !found {
		return DefaultWebAddress
	}
	return address
}

// GetDatastoreDir returns a directory specified in the arguments via "dir=..." or
// defaults to the current directory.
func (cmd Command) GetDatastoreDir() string {
	datastoreDir, found := cmd.GetParameter("dir")
	if !found {
		currentDir, err := os.Getwd()
		if err != nil {
			log.Fatalln("Could not get current directory:", err)
		}
		return currentDir
	}
	return datastoreDir
}

// Types returns a chart of supported data types for this DVID datastore
func (cmd Command) Types(reply *Param) error {
	reply.Text = dataService.Config.SupportedTypeChart()
	return nil
}
