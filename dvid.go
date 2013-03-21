package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/terminal"

	// Declare the data types this DVID executable will support
	_ "github.com/janelia-flyem/dvid/datatype/grayscale8"
)

var (
	// Display usage if true.
	showHelp = flag.Bool("help", false, "")

	// List the supported data types if true.
	showTypes = flag.Bool("types", false, "")

	// Run in debug mode if true.
	runDebug = flag.Bool("debug", false, "")

	// Run in benchmark mode if true.
	runBenchmark = flag.Bool("benchmark", false, "")

	// Profile CPU usage using standard gotest system. 
	cpuprofile = flag.String("cpuprofile", "", "")

	// Profile memory usage using standard gotest system. 
	memprofile = flag.String("memprofile", "", "")

	// Path to web client directory.  Leave unset for default pages.
	clientDir = flag.String("webclient", "", "")

	// Number of logical CPUs to use for DVID.  
	useCPU = flag.Int("numcpu", 0, "")

	// Size of DVID data cache
	cacheMBytes = flag.Uint64("cache", datastore.DefaultCacheMBytes, "")
)

const helpMessage = `
dvid is a distributed, versioned image datastore

Usage: dvid [options] <command>

      -numcpu     =number   Number of logical CPUs to use for DVID.
      -cache      =number   Megabytes of LRU cache for blocks.  (Default: %d MB)
      -webclient  =string   Path to web client directory.  Leave unset for default pages.
      -cpuprofile =string   Write CPU profile to this file.
      -memprofile =string   Write memory profile to this file on ctrl-C.
      -types      (flag)    Show compiled DVID data types
      -debug      (flag)    Run in debug mode.  Verbose.
      -benchmark  (flag)    Run in benchmarking mode. 
  -h, -help       (flag)    Show help message

  For profiling, please refer to this excellent article:
  http://blog.golang.org/2011/06/profiling-go-programs.html

Commands that can be performed without a running server:

	version
	init [config=/path/to/json/config] [dir=/path/to/datastore/dir]
	serve [dir=/path/to/datastore/dir] [web=...] [rpc=...]
`

const helpServerMessage = `
For further information, launch the DVID server (enter "dvid serve"), then use
a web browser to visit the DVID web server ("localhost:4000" by default).
`

var usage = func() {
	// Print local DVID help
	fmt.Printf(helpMessage, datastore.DefaultCacheMBytes)

	// Print server DVID help if available
	err := DoCommand(dvid.Command([]string{"help"}))
	if err != nil {
		fmt.Println(helpServerMessage)
	}
}

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}
	if *showTypes {
		fmt.Println(datastore.CompiledTypeChart())
		os.Exit(0)
	}

	if *runDebug {
		dvid.Mode = dvid.Debug
	}
	if *runBenchmark {
		dvid.Mode = dvid.Benchmark
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
	dvidCPU := numCPU
	if *useCPU != 0 {
		dvidCPU = *useCPU
	}
	runtime.GOMAXPROCS(dvidCPU)
	log.Printf("Using %d of %d logical CPUs for DVID.\n", dvidCPU, numCPU)

	// Setup the DVID data cache.
	cacheBytes := *cacheMBytes * dvid.Mega
	log.Printf("Using %d MB for DVID data cache.\n", *cacheMBytes)
	datastore.InitDataCache(cacheBytes)

	// Capture ctrl+c and handle graceful shutdown (flushing of cache, etc.)
	ctrl_c := make(chan os.Signal, 1)
	go func() {
		for sig := range ctrl_c {
			log.Printf("Captured %v.  Shutting down...\n", sig)
			if *memprofile != "" {
				log.Printf("Storing memory profiling to %s...\n", *memprofile)
				f, err := os.Create(*memprofile)
				if err != nil {
					log.Fatal(err)
				}
				pprof.WriteHeapProfile(f)
				f.Close()
			}
			log.Println("Flushing cache to datastore...\n")
			datastore.Shutdown()
			if *cpuprofile != "" {
				log.Printf("Stopping CPU profiling to %s...\n", *cpuprofile)
				pprof.StopCPUProfile()
			}
			os.Exit(1)
		}
	}()
	signal.Notify(ctrl_c, os.Interrupt)

	// If we have no arguments, run in terminal mode, else execute command.
	if flag.NArg() == 0 {
		terminal.Shell()
	} else {
		dvid.Fmt(dvid.Debug, "Running in Debug mode\n")
		dvid.Fmt(dvid.Benchmark, "Running in Benchmark mode\n")
		command := dvid.Command(flag.Args())
		if err := DoCommand(command); err != nil {
			fmt.Println(err.Error())
		}
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
	case "init":
		return DoInit(cmd)
	case "serve":
		return DoServe(cmd)
	case "version":
		fmt.Println(datastore.Versions())
	// Send everything else to server via DVID terminal
	default:
		return terminal.Send(cmd)
	}
	return nil
}

// DoInit performs the "init" command, creating a new DVID datastore.
func DoInit(cmd dvid.Command) error {
	configFile, _ := cmd.Parameter(dvid.KeyConfigFile)
	datastoreDir := cmd.DatastoreDir()

	create := true
	uuid := datastore.Init(datastoreDir, configFile, create)
	fmt.Println("Root node UUID:", uuid)
	return nil
}

// DoServe opens a datastore then creates both web and rpc servers for the datastore
func DoServe(cmd dvid.Command) error {

	webAddress, _ := cmd.Parameter(dvid.KeyWeb)
	rpcAddress, _ := cmd.Parameter(dvid.KeyRpc)
	datastoreDir := cmd.DatastoreDir()

	if err := server.Serve(datastoreDir, webAddress, *clientDir, rpcAddress); err != nil {
		return err
	}
	return nil
}
