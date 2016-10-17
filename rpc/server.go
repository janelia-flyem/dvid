/*
	This file implements high-throughput messaging using gorpc.
*/

package rpc

import (
	"errors"
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/valyala/gorpc"
)

const (
	// The default address for internal messaging to this DVID server
	DefaultAddress = "localhost:8002"
)

var (
	// dispatcher provides wrapper to route calls.
	dispatcher *gorpc.Dispatcher

	// servers assigned to different ports
	servers map[string]*gorpc.Server

	// keep track of sessions so incoming messages are routed to
	// appropriate session.
	handlers map[SessionID]SessionHandler

	// new session ID
	sessionID SessionID

	// when a given message is received from client, create new session
	// using functions provided by various packages.
	registered map[MessageID]NewSessionHandlerFunc

	registeredMu, handlersMu sync.RWMutex
)

var (
	ErrNoServerRunning     = errors.New("no servers are running")
	ErrServerNotFound      = errors.New("server not found")
	ErrClientUninitialized = errors.New("client not initialized")
	ErrBadSession          = errors.New("bad session id; not found on server")
)

var (
	sendNewSession = "NewSession"
	sendEndSession = "EndSession"
)

func init() {
	registered = make(map[MessageID]NewSessionHandlerFunc)
	handlers = make(map[SessionID]SessionHandler)

	// Create dispatcher for session creation and other functions.
	d := gorpc.NewDispatcher()
	d.AddFunc(sendNewSession, newSession)
	d.AddFunc(sendEndSession, endSession)
	dispatcher = d

	var m MessageID
	gorpc.RegisterType(m)
	var s SessionID
	gorpc.RegisterType(s)
}

func Dispatcher() *gorpc.Dispatcher {
	return dispatcher
}

// Transmit describes how data is being transferred from DVID-to-DVID.
type Transmit uint8

const (
	TransmitUnknown Transmit = iota
	TransmitAll              // send all versions to maintain the full DAG
	TransmitBranch           // send versions in ancestor path that are not in local DAG
	TransmitFlatten          // send just the version required and do required copy from earlier versions
)

// SessionID uniquely identifies a rpc session.
type SessionID uint64

type MessageHandler func(interface{}) (interface{}, error)

// SessionHandler has a state, processes incoming messages via a finite state
// machine, and knows how to terminate itself upon receiving a termination
// message, calling the rpc package EndSession.
type SessionHandler interface {
	ID() SessionID
	Open(SessionID) error
	Close() error
}

// NewSessionHandlerFunc is a function that creates new session handlers
type NewSessionHandlerFunc func(MessageID) (SessionHandler, error)

// RegisterSessionMaker allows any package to register a function to create
// a new session handler given a designated message type.
func RegisterSessionMaker(id MessageID, f NewSessionHandlerFunc) {
	registeredMu.Lock()
	registered[id] = f
	registeredMu.Unlock()
}

func newSession(msgID MessageID) (SessionID, error) {
	registeredMu.RLock()
	defer registeredMu.RUnlock()

	sessionMaker, found := registered[msgID]
	if !found {
		return 0, fmt.Errorf("unknown message ID for session: %q", msgID)
	}
	if sessionMaker == nil {
		return 0, fmt.Errorf("no session maker found for %q", msgID)
	}

	handler, err := sessionMaker(msgID)
	if err != nil {
		return 0, err
	}

	handlersMu.Lock()
	sessionID++
	handlers[sessionID] = handler
	handlersMu.Unlock()

	if err := handler.Open(sessionID); err != nil {
		return 0, err
	}

	return sessionID, nil
}

func endSession(id SessionID) error {
	handlersMu.Lock()
	defer handlersMu.Unlock()

	handler, found := handlers[id]
	if !found {
		return ErrBadSession
	}
	if err := handler.Close(); err != nil {
		return err
	}
	delete(handlers, id)
	return nil
}

func GetSessionHandler(sid SessionID) (SessionHandler, error) {
	// Get the session from the id.
	handlersMu.RLock()
	defer handlersMu.RUnlock()

	handler, found := handlers[sid]
	if !found {
		return nil, ErrBadSession
	}
	return handler, nil
}

// StartServer starts an RPC server.
func StartServer(address string) error {
	gorpc.SetErrorLogger(dvid.Errorf) // Send gorpc errors to appropriate error log.

	s := gorpc.NewTCPServer(address, dispatcher.NewHandlerFunc())
	if servers == nil {
		servers = make(map[string]*gorpc.Server)
	}
	servers[address] = s
	return s.Serve()
}

// StopServer halts the given server.
func StopServer(address string) error {
	if servers == nil {
		return ErrNoServerRunning
	}
	s, found := servers[address]
	if !found {
		return ErrServerNotFound
	}
	delete(servers, address)
	s.Stop()
	return nil
}

// Shutdown halts all RPC servers.
func Shutdown() {
	for _, s := range servers {
		s.Stop()
	}
	dvid.Infof("Halted %d RPC servers.\n", len(servers))
	servers = nil
}
