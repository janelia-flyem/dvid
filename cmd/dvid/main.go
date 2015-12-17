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
	_ "github.com/janelia-flyem/dvid/datatype/labelblk"
	_ "github.com/janelia-flyem/dvid/datatype/labelgraph"
	_ "github.com/janelia-flyem/dvid/datatype/labelvol"
	_ "github.com/janelia-flyem/dvid/datatype/multichan16"
	_ "github.com/janelia-flyem/dvid/datatype/roi"
	_ "github.com/janelia-flyem/dvid/datatype/synapse"
	//_ "github.com/janelia-flyem/dvid/datatype/labelsurf"
	//_ "github.com/janelia-flyem/dvid/datatype/labelsz"
)

var (
	// Display usage if true.
	showHelp = flag.Bool("help", false, "")

	// Read-only server.  Will only allow GET and HEAD requests.
	readonly = flag.Bool("readonly", false, "")

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
  -h, -help       (flag)    Show help message

Commands that can be performed without a running server:

	about
	help
	serve  <configuration path>

For storage engines that have repair ability (e.g., basholeveldb):

	repair <engine name> <database path>

		The <engine name> refers to the name of the engine: "basholeveldb", "kvautobus", etc.
		The <database path> is the file path to the directory.
`

var usage = func() {
	// Print local DVID help
	fmt.Printf(helpMessage)

	// Print server DVID help if available
	err := DoCommand(dvid.Command([]string{"help"}))
	if err != nil {
		fmt.Printf("Unable to get 'help' from DVID server at %q.\n%v\n",
			server.DefaultWebAddress, err)
	}
}

func currentDir() string {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalln("Could not get current directory:", err)
	}
	return currentDir
}

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() >= 1 && strings.ToLower(flag.Args()[0]) == "help" {
		*showHelp = true
	}

	if *runVerbose {
		dvid.Verbose = true
	}
	if *showHelp || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(0)
	}
	if *readonly {
		server.SetReadOnly(true)
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
		client, err := server.NewClient(*rpcAddress)
		if err != nil {
			return err
		}
		request := datastore.Request{Command: cmd}
		if *useStdin {
			var err error
			request.Input, err = ioutil.ReadAll(os.Stdin)
			if err != nil {
				return fmt.Errorf("Error in reading from standard input: %v", err)
			}
		}
		return client.Send(request)
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
	instanceConfig, logConfig, storeConfig, err := server.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("Error loading configuration file %q: %v\n", configPath, err)
	}
	logConfig.SetLogger()

	// Initialize storage and datastore layer
	initMetadata, err := storage.Initialize(cmd.Settings(), storeConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize storage: %v\n", err)
	}
	if err := datastore.Initialize(initMetadata, instanceConfig); err != nil {
		return fmt.Errorf("Unable to initialize datastore: %v\n", err)
	}

	// add handlers to help us track memory usage - they don't track memory until they're told to
	profiler.AddMemoryProfilingHandlers()

	// Uncomment if you want to start profiling automatically
	// profiler.StartProfiling()

	// listen on port 6060 (pick a port) for profiling.
	go http.ListenAndServe(":6060", nil)

	// Serve HTTP and RPC
	if err := server.Serve(); err != nil {
		return err
	}
	return nil
}
