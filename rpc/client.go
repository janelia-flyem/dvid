/*
	This file implements high-throughput messaging using gorpc.
*/

package rpc

import (
	"fmt"

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
	return Session{c, dc, sid}, nil
}

func (s *Session) ID() SessionID {
	return s.id
}

func (s *Session) Call() Caller {
	return s.dc.Call
}

func (s *Session) Close() error {
	_, err := s.dc.Call(sendEndSession, s.id)
	s.c.Stop()
	return err
}
