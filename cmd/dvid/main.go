// Command-line interface to a remote DVID server.
// Provides essential commands on top of core http server: init, serve, repair.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/janelia-flyem/go/profiler"

	// Declare the data types this DVID executable will support
	_ "github.com/janelia-flyem/dvid/datatype/annotation"
	_ "github.com/janelia-flyem/dvid/datatype/googlevoxels"
	_ "github.com/janelia-flyem/dvid/datatype/imageblk"
	_ "github.com/janelia-flyem/dvid/datatype/imagetile"
	_ "github.com/janelia-flyem/dvid/datatype/keyvalue"
	_ "github.com/janelia-flyem/dvid/datatype/labelarray"
	_ "github.com/janelia-flyem/dvid/datatype/labelblk"
	_ "github.com/janelia-flyem/dvid/datatype/labelgraph"
	_ "github.com/janelia-flyem/dvid/datatype/labelmap"
	_ "github.com/janelia-flyem/dvid/datatype/labelsz"
	_ "github.com/janelia-flyem/dvid/datatype/labelvol"
	_ "github.com/janelia-flyem/dvid/datatype/multichan16"
	_ "github.com/janelia-flyem/dvid/datatype/roi"
	_ "github.com/janelia-flyem/dvid/datatype/tarsupervoxels"
)

var (
	// Display usage if true.
	showHelp = flag.Bool("help", false, "")

	// Read-only server.  Will only allow GET and HEAD requests.
	readonly = flag.Bool("readonly", false, "")

	// Fully writable server, including locked nodes.  [Expert use only.]
	fullwrite = flag.Bool("fullwrite", false, "")

	// Run in verbose mode if true.
	runVerbose = flag.Bool("verbose", false, "")

	rpcAddress = flag.String("rpc", server.DefaultRPCAddress, "")

	// msgAddress = flag.String("message", message.DefaultAddress, "")

	// Profile CPU usage using standard gotest system.
	cpuprofile = flag.String("cpuprofile", "", "")

	// Profile memory usage using standard gotest system.
	memprofile = flag.String("memprofile", "", "")

	// Number of logical CPUs to use for DVID.
	useCPU = flag.Int("numcpu", 0, "")

	// Accept and send stdin to server for use in commands if true.
	useStdin = flag.Bool("stdin", false, "")
)

const helpMessage = `

dvid is a command-line interface to a distributed, versioned image-oriented datastore

Usage: dvid [options] <command>

      -readonly   (flag)    HTTP API ignores anything but GET and HEAD requests.
      -rpc        =string   Address for RPC communication.
      -cpuprofile =string   Write CPU profile to this file.
      -memprofile =string   Write memory profile to this file on ctrl-C.
      -numcpu     =number   Number of logical CPUs to use for DVID.
      -stdin      (flag)    Accept and send stdin to server for use in commands.
      -verbose    (flag)    Run in verbose mode.
      -fullwrite  (flag)    Allow limited ops to all nodes, even committed ones. [Limit to expert use]
  -h, -help       (flag)    Show help message

Commands that can be performed without a running server:

    about
    help
    serve  <configuration path>

For storage engines that have repair ability (e.g., basholeveldb):

    repair <engine name> <database path>

        The <engine name> refers to the name of the engine: "basholeveldb", "kvautobus", etc.
        The <database path> is the file path to the directory.

To get help for a remote DVID server:

    help server

        Use the -rpc flag to set the remote DVID rpc port if not the default.

`

var usage = func() {
	// Print local DVID help
	fmt.Printf(helpMessage)
}

func currentDir() string {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalln("Could not get current directory:", err)
	}
	return currentDir
}

func main() {
	defer func() {
		if e := recover(); e != nil {
			msg := fmt.Sprintf("Panic detected on main serve thread: %+v\n", e)
			dvid.ReportPanic(msg, server.WebServer())
		}
	}()

	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(0)
	}

	help := strings.ToLower(flag.Args()[0]) == "help"

	if help && flag.NArg() == 1 {
		*showHelp = true
	}
	if *showHelp || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(0)
	}

	if help && flag.NArg() == 2 && strings.ToLower(flag.Args()[0]) == "server" {
		if err := DoCommand(dvid.Command([]string{"help"})); err != nil {
			fmt.Printf("Unable to get 'help' from DVID server at %q.\n%v\n", rpcAddress, err)
		}
		os.Exit(0)
	}

	if *runVerbose {
		dvid.SetLogMode(dvid.DebugMode)
	}

	if *fullwrite && *readonly {
		fmt.Printf("Can only do -fullwrite or -readonly, not both.\n\n")
		flag.Usage()
		os.Exit(0)
	}

	if *readonly {
		server.SetReadOnly(true)
	}
	if *fullwrite {
		server.SetFullWrite(true)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Determine numer of logical CPUs on local machine and unless overridden, use
	// all of them.
	numCPU := runtime.NumCPU()
	if *useCPU != 0 {
		dvid.NumCPU = *useCPU
	} else if flag.NArg() >= 1 && flag.Args()[0] == "serve" {
		dvid.NumCPU = numCPU
	} else {
		dvid.NumCPU = 1
	}
	runtime.GOMAXPROCS(dvid.NumCPU)

	command := dvid.Command(flag.Args())
	if err := DoCommand(command); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

// DoCommand serves as a switchboard for commands, handling local ones and
// sending via rpc those commands that need a running server.
func DoCommand(cmd dvid.Command) error {
	if len(cmd) == 0 {
		return fmt.Errorf("Blank command!")
	}

	switch cmd.Name() {
	// Handle commands that don't require server connection
	case "serve":
		return DoServe(cmd)
	case "repair":
		return DoRepair(cmd)
	case "about":
		fmt.Println(server.About())
	// Send everything else to server via DVID terminal
	default:
		request := datastore.Request{Command: cmd}
		if *useStdin {
			var err error
			request.Input, err = ioutil.ReadAll(os.Stdin)
			if err != nil {
				return fmt.Errorf("Error in reading from standard input: %v", err)
			}
		}
		return server.SendRPC(*rpcAddress, request)
	}
	return nil
}

// DoRepair performs the "repair" command, trying to repair a storage engine
func DoRepair(cmd dvid.Command) error {
	engineName := cmd.Argument(1)
	path := cmd.Argument(2)
	if path == "" {
		return fmt.Errorf("repair command must be followed by engine name and path to the datastore")
	}
	if err := storage.Repair(engineName, path); err != nil {
		return err
	}
	fmt.Printf("Ran repair on %q database at %s.\n", engineName, path)
	return nil
}

// DoServe opens a datastore then creates both web and rpc servers for the datastore
func DoServe(cmd dvid.Command) error {
	// Capture ctrl+c and other interrupts.  Then handle graceful shutdown.
	stopSig := make(chan os.Signal)
	go func() {
		for sig := range stopSig {
			log.Printf("Stop signal captured: %q.  Shutting down...\n", sig)
			if *memprofile != "" {
				log.Printf("Storing memory profiling to %s...\n", *memprofile)
				f, err := os.Create(*memprofile)
				if err != nil {
					log.Fatal(err)
				}
				pprof.WriteHeapProfile(f)
				f.Close()
			}
			if *cpuprofile != "" {
				log.Printf("Stopping CPU profiling to %s...\n", *cpuprofile)
				pprof.StopCPUProfile()
			}
			server.Shutdown()
			time.Sleep(1 * time.Second)
			os.Exit(0)
		}
	}()
	signal.Notify(stopSig, os.Interrupt, os.Kill, syscall.SIGTERM)

	// Load server configuration.
	configPath := cmd.Argument(1)
	if configPath == "" {
		return fmt.Errorf("serve command must be followed by the path to the TOML configuration file")
	}
	if err := server.LoadConfig(configPath); err != nil {
		return fmt.Errorf("error loading configuration file %q: %v", configPath, err)
	}

	if err := server.Initialize(); err != nil {
		return err
	}

	// Initialize storage and datastore layer
	backend, err := server.GetBackend()
	if err != nil {
		return err
	}
	initMetadata, err := storage.Initialize(cmd.Settings(), backend)
	if err != nil {
		return fmt.Errorf("unable to initialize storage: %v", err)
	}

	// lock metadata
	store, _ := storage.MetaDataKVStore()
	transdb, hastrans := store.(storage.TransactionDB)
	if hastrans {
		var ctx storage.MetadataContext
		key := ctx.ConstructKey(storage.NewTKey(datastore.ServerLockKey, nil))
		transdb.LockKey(key)
	}

	if err := datastore.Initialize(initMetadata, server.DatastoreConfig()); err != nil {
		if hastrans {
			var ctx storage.MetadataContext
			key := ctx.ConstructKey(storage.NewTKey(datastore.ServerLockKey, nil))
			transdb.UnlockKey(key)
		}
		return fmt.Errorf("unable to initialize datastore: %v", err)
	}
	if hastrans {
		var ctx storage.MetadataContext
		key := ctx.ConstructKey(storage.NewTKey(datastore.ServerLockKey, nil))
		transdb.UnlockKey(key)
	}

	// add handlers to help us track memory usage - they don't track memory until they're told to
	profiler.AddMemoryProfilingHandlers()

	// Uncomment if you want to start profiling automatically
	// profiler.StartProfiling()

	// listen on port 6060 (pick a port) for profiling.
	go http.ListenAndServe(":6060", nil)

	// Serve HTTP and RPC
	server.Serve()
	return nil
}
