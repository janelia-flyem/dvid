package server

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
)

const DefaultWebAddress = "localhost:4000"
const DefaultRpcAddress = "localhost:6000"

// Command is a type exported via the rpc server
type Command struct {
	// Args lists the elements of the command where Args[0] is the command string
	Args []string
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

func (cmd *Command) Do(args *Command, reply *string) error {
	// TODO FIX BELOW
	// Handle builtin commands
	switch cmd.Args[0] {
	case "branch", "lock", "types", "log", "pull", "push":
		return fmt.Errorf("Command '%s' has not been implemented yet", cmd.Args[0])
	default:
		// Check if this is a supported data type name

		// If it's not supported, return error.
		return fmt.Errorf("Command is not built-in command or supported data type: %s", cmd)
	}
}

// Handler for main page
func mainHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "<h1>DVID Server Main Page</h1>")
}

// Handler for API commands through HTTP
func apiHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "<h1>DVID Server API Handler ...</h1>")

	const lenPath = len("/api/")
	url := r.URL.Path[lenPath:]

	fmt.Fprintln(w, "<p>Processing", url, "</p>")
}

// Serve opens a datastore then creates both web and rpc servers for the datastore
func Serve(datastoreDir, webAddress, rpcAddress string) error {

	// Get exclusive ownership of a DVID datastore
	dataService, err := datastore.Open(datastoreDir)
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Make sure we can support the datastore's types with our current DVID executable
	err = dataService.Config.VerifySupportedTypes()
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Launch the web server
	go ServeHttp(webAddress, dataService)

	// Launch the rpc server
	err = ServeRpc(rpcAddress, dataService)
	if err != nil {
		log.Fatalln(err.Error())
	}

	return nil
}

// Listen and serve HTTP requests using address and don't let stay-alive
// connections hog goroutines for more than an hour.
// See for discussion: 
// http://stackoverflow.com/questions/10971800/golang-http-server-leaving-open-goroutines
func ServeHttp(address string, dataService *datastore.Service) {

	fmt.Printf("Web server listening at %s ...\n", address)

	if address == "" {
		address = DefaultWebAddress
	}
	src := &http.Server{
		Addr:        address,
		ReadTimeout: 1 * time.Hour,
	}
	http.HandleFunc("/", mainHandler)
	http.HandleFunc("/api/", apiHandler)
	src.ListenAndServe()
}

// Listen and serve RPC requests using address.
func ServeRpc(address string, dataService *datastore.Service) error {

	fmt.Printf("Rpc server listening at %s ...\n", address)

	if address == "" {
		address = DefaultRpcAddress
	}
	rpcCommand := new(Command)
	rpc.Register(rpcCommand)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	http.Serve(listener, nil)
	return nil
}
