/*
	This file implements messaging using RPC -- slow but predictable :)
*/

package message

import (
	"fmt"
	"net/rpc"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

const (
	// The default address for internal messaging to this DVID server
	DefaultAddress = "localhost:8002"
)

type RPCConnection struct {
	Address string
}

// ReceiveMessage accepts a message from a client and either starts a new session
// or passes the message to an existing session.
func (c *RPCConnection) ReceiveMessage(m Message, ok *bool) error {
	// Handle command starts
	if m.Type == CommandType {
		registeredOps.RLock()
		defer registeredOps.RUnlock()
		sessionCreator, found := registeredOps.commands[m.Name]
		if found {
			// Get a new session.
			// TODO -- handle concurrent pipeline requests
			session, err := sessionCreator(&m)
			if err != nil {
				return fmt.Errorf("Error handling command %q: %v\n", m.Name, err)
			}

			// Save the session
			// TODO -- check there's not already command ongoing for this session
			sessions[m.Session] = session
			*ok = true
			return nil
		}
	}

	// Pass other messages to existing session
	session, found := sessions[m.Session]
	if !found {
		return fmt.Errorf("Received %s message before any command establishing session", m.Type)
	}
	if err := session.ProcessMessage(&m); err != nil {
		return err
	}

	*ok = true
	return nil
}

// Returns an RPC "socket" that can push data to remote.
func NewPushSocket(target string) (*rpcSocket, error) {
	client, err := rpc.DialHTTP("tcp", target)
	if err != nil {
		return nil, fmt.Errorf("Could not find DVID server to message at %s [%v]\n", target, err)
	}
	msg := Message{Session: string(dvid.NewUUID())}
	return &rpcSocket{target, client, msg}, nil
}

type rpcSocket struct {
	rpcAddress string
	client     *rpc.Client
	msg        Message
}

func (s *rpcSocket) sendMessage() error {
	var ok bool
	if s.client != nil {
		err := s.client.Call("RPCConnection.ReceiveMessage", s.msg, &ok)
		if err != nil {
			return fmt.Errorf("RPC error for message %s: %v", s.msg.Type, err)
		}
		if !ok {
			return fmt.Errorf("Bad send message for %s detected on client-side!", s.msg.Type)
		}
	}
	return nil
}

func (s *rpcSocket) SendCommand(command string) error {
	s.msg.Type = CommandType
	s.msg.Name = command
	return s.sendMessage()
}

func (s *rpcSocket) SendPostProc(command string, data []byte) error {
	s.msg.Type = PostProcType
	s.msg.Name = command
	s.msg.Data = data
	return s.sendMessage()
}

func (s *rpcSocket) SendKeyValue(desc string, store storage.DataStoreType, kv *storage.KeyValue) error {
	s.msg.Type = KeyValueType
	s.msg.SType = store
	s.msg.KV = kv
	return s.sendMessage()
}

func (s *rpcSocket) SendBinary(desc string, data []byte) error {
	s.msg.Type = BinaryType
	s.msg.Name = desc
	s.msg.Data = data
	return s.sendMessage()
}
