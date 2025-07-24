/*
	This file handles Arrow Flight server connections for high-performance data streaming,
	primarily used by Python workers to receive labelmap export data.
*/

package server

import (
	"fmt"
	"net"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"

	"github.com/apache/arrow/go/v14/arrow/flight"
	"google.golang.org/grpc"
)

const (
	// DefaultFlightAddress is the default address for the Arrow Flight server
	DefaultFlightAddress = "localhost:8002"
)

var (
	// flightServer holds the global Arrow Flight server instance
	flightServer *DVIDFlightServer
	
	// flightServerMutex protects access to flightServer
	flightServerMutex sync.RWMutex
)

// DVIDFlightServer implements the Arrow Flight server for DVID
type DVIDFlightServer struct {
	flight.BaseFlightServer
	
	// connectedWorkers tracks active worker connections
	connectedWorkers map[string]*WorkerConnection
	workerMutex      sync.RWMutex
	
	// server holds the gRPC server instance
	server *grpc.Server
	
	// address is the server listening address
	address string
}

// WorkerConnection represents a connected Python worker
type WorkerConnection struct {
	ID       string
	Stream   flight.FlightService_DoExchangeServer
	Connected bool
}

// StartFlightServer initializes and starts the Arrow Flight server
func StartFlightServer(address string) error {
	if address == "" {
		address = DefaultFlightAddress
	}
	
	dvid.Infof("Starting Arrow Flight server on %s\n", address)
	
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("Arrow Flight server failed to listen on %s: %v", address, err)
	}
	
	flightServerMutex.Lock()
	flightServer = &DVIDFlightServer{
		connectedWorkers: make(map[string]*WorkerConnection),
		address:         address,
	}
	flightServerMutex.Unlock()
	
	// Create gRPC server with Arrow Flight service
	server := grpc.NewServer()
	flight.RegisterFlightServiceServer(server, flightServer)
	flightServer.server = server
	
	dvid.Infof("Arrow Flight server listening on %s\n", address)
	
	// Start serving (this blocks)
	if err := server.Serve(listener); err != nil {
		return fmt.Errorf("Arrow Flight server failed to serve: %v", err)
	}
	
	return nil
}

// GetFlightServer returns the global Arrow Flight server instance
func GetFlightServer() *DVIDFlightServer {
	flightServerMutex.RLock()
	defer flightServerMutex.RUnlock()
	return flightServer
}

// GetConnectedWorkerCount returns the number of connected Python workers
func (s *DVIDFlightServer) GetConnectedWorkerCount() int {
	s.workerMutex.RLock()
	defer s.workerMutex.RUnlock()
	
	count := 0
	for _, worker := range s.connectedWorkers {
		if worker.Connected {
			count++
		}
	}
	return count
}

// DoExchange handles bidirectional streaming with Python workers
func (s *DVIDFlightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	// Generate worker ID from stream context
	workerID := string(dvid.NewUUID())
	
	dvid.Infof("Arrow Flight worker %s connected\n", workerID)
	
	// Register the worker
	worker := &WorkerConnection{
		ID:        workerID,
		Stream:    stream,
		Connected: true,
	}
	
	s.workerMutex.Lock()
	s.connectedWorkers[workerID] = worker
	s.workerMutex.Unlock()
	
	// Handle the connection (this blocks until worker disconnects)
	defer func() {
		s.workerMutex.Lock()
		if w, exists := s.connectedWorkers[workerID]; exists {
			w.Connected = false
		}
		s.workerMutex.Unlock()
		dvid.Infof("Arrow Flight worker %s disconnected\n", workerID)
	}()
	
	// Keep connection alive and handle incoming messages
	for {
		_, err := stream.Recv()
		if err != nil {
			dvid.Infof("Arrow Flight worker %s stream ended: %v\n", workerID, err)
			break
		}
		// Process incoming messages from Python workers if needed
	}
	
	return nil
}

// SendDataToWorker sends Arrow data to a specific worker
func (s *DVIDFlightServer) SendDataToWorker(workerID string, data *flight.FlightData) error {
	s.workerMutex.RLock()
	worker, exists := s.connectedWorkers[workerID]
	s.workerMutex.RUnlock()
	
	if !exists || !worker.Connected {
		return fmt.Errorf("Worker %s not connected", workerID)
	}
	
	return worker.Stream.Send(data)
}

// GetWorkerIDs returns a list of connected worker IDs
func (s *DVIDFlightServer) GetWorkerIDs() []string {
	s.workerMutex.RLock()
	defer s.workerMutex.RUnlock()
	
	var workerIDs []string
	for id, worker := range s.connectedWorkers {
		if worker.Connected {
			workerIDs = append(workerIDs, id)
		}
	}
	return workerIDs
}

// Shutdown gracefully shuts down the Arrow Flight server
func (s *DVIDFlightServer) Shutdown() {
	if s.server != nil {
		dvid.Infof("Shutting down Arrow Flight server\n")
		s.server.GracefulStop()
	}
}