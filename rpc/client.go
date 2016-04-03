/*
	This file implements high-throughput messaging using gorpc.
*/

package rpc

import (
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/valyala/gorpc"
)

// MessageID should be a unique string across the DVID code base.
// By convention, the package name should be prefixed to the type
// of session being performed, e.g., "datastore.Push"
type MessageID string

// Caller is a function that meets the rpc Call signature.
type Caller func(string, interface{}) (interface{}, error)

// Session provides ability to send data to remote DVID using multiple
// RPCs.
type Session struct {
	sync.WaitGroup
	c  *gorpc.Client
	dc *gorpc.DispatcherClient
	id SessionID
}

// NewSession returns a new session to the remote address where the
// type of session is reflected by the MessageID.
func NewSession(addr string, mid MessageID) (Session, error) {
	c := gorpc.NewTCPClient(addr)
	c.Start()
	dc := dispatcher.NewFuncClient(c)
	if dc == nil {
		return Session{}, fmt.Errorf("can't create dispatcher client")
	}

	// Start session create RPC using the given message, which
	// should remotely use the registered session maker and return
	// a session ID.
	resp, err := dc.Call(sendNewSession, mid)
	if err != nil {
		return Session{}, err
	}
	sid, ok := resp.(SessionID)
	if !ok {
		return Session{}, fmt.Errorf("remote server returned %v instead of session id", resp)
	}

	// Create the session.
	return Session{c: c, dc: dc, id: sid}, nil
}

func (s *Session) ID() SessionID {
	return s.id
}

func (s *Session) Call() Caller {
	return s.dc.Call
}

// StartJob marks the start of a sequence of calls that may
// be handled concurrently.  A session will wait for StopJob()
// before allowing proceeding with any closing of the session.
func (s *Session) StartJob() {
	s.Add(1)
}

func (s *Session) StopJob() {
	s.Done()
}

func (s *Session) Close() error {
	dvid.Debugf("session %d close: waiting for any jobs to complete...\n", s.id)
	s.Wait()
	dvid.Debugf("sending session end to remote...\n")
	_, err := s.dc.Call(sendEndSession, s.id)
	dvid.Debugf("stopping client...\n")
	s.c.Stop()
	return err
}
