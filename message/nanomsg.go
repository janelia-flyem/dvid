// +build nanomsg

/*
	EXPERIMENTAL -- needs more work to make messaging robust due to drops.

	This file implements messaging using nanomsg.
*/

package message

import (
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/janelia-flyem/go/go-nanomsg"
)

const (
	// Default nanomsg REP server is in-process, allowing pushes within a DVID server
	// to create copies.
	DefaultNanomsgAddress = "inproc://a"
)

func NewPullSocket(address string) (*nanoSocket, error) {
	s, err := nanomsg.NewSocket(nanomsg.AF_SP, nanomsg.PULL)
	if err != nil {
		return nil, fmt.Errorf("Unable to create new nanomsg PULL socket: %s\n", err.Error())
	}
	_, err = s.Bind(address)
	if err != nil {
		return nil, fmt.Errorf("Unable to bind nanomsg to %q\n", address)
	}
	return &nanoSocket{s}, nil
}

func NewPushSocket(target string) (*nanoSocket, error) {
	s, err := nanomsg.NewSocket(nanomsg.AF_SP, nanomsg.PUSH)
	if err != nil {
		return nil, err
	}
	_, err = s.Connect(target)
	if err != nil {
		return nil, err
	}
	return &nanoSocket{s}, nil
}

func ServeNanomsg(address string) {
	dvid.Infof("Starting nanomsg receiver on %v\n", address)
	var err error
	incoming, err = NewPullSocket(address)
	if err != nil {
		dvid.Errorf("Unable to get new pull socket: %s\n", err.Error())
		return
	}
	for {
		cmd, err := incoming.ReceiveCommand()
		dvid.Debugf("Received command over nanomsg address %q: %s\n", address, cmd)
		if err != nil {
			dvid.Errorf("Bad receive on nanomsg address %q: %s\n", address, err.Error())
			break
		}
		registeredOps.RLock()
		defer registeredOps.RUnlock()
		callback, found := registeredOps.commands[cmd]
		if !found {
			dvid.Errorf("Received unregistered command: %s\n", cmd)
			break
		}

		// Handle the operation
		// TODO -- handle concurrent pipeline requests
		if err = callback(incoming); err != nil {
			dvid.Errorf("Error handling op %q: %s\n", cmd, err.Error())
			break
		}
	}
}

type nanoSocket struct {
	*nanomsg.Socket
}

func (s *nanoSocket) receiveOp() (*Op, error) {
	received, err := s.Recv(0)
	if err != nil {
		return nil, err
	}
	var op Op
	if err = op.UnmarshalBinary(received); err != nil {
		return nil, err
	}
	return &op, nil
}

func (s *nanoSocket) receiveKeyValue() (stype storage.DataStoreType, kv *storage.KeyValue, err error) {
	var received []byte
	received, err = s.Recv(0)
	if err != nil {
		return
	}
	if len(received) != 1 {
		err = fmt.Errorf("Expected store type byte and received %d bytes", len(received))
		return
	}
	stype = storage.DataStoreType(received[0])
	kv = new(storage.KeyValue)
	kv.K, err = s.Recv(0)
	if err != nil {
		return
	}
	kv.V, err = s.Recv(0)
	return
}

func (s *nanoSocket) receiveBinary() ([]byte, error) {
	return s.Recv(0)
}

// ReceiveMessage returns whatever kind of data is sent.
func (s *nanoSocket) ReceiveMessage() (*Message, error) {
	op, err := s.receiveOp()
	if err != nil {
		return nil, err
	}
	msg := &Message{Type: op.optype, Name: op.name}
	switch op.optype {
	case CommandType:
		// nothing needed
	case PostProcType:
		msg.Data, err = s.receiveBinary()
	case KeyValueType:
		msg.SType, msg.KV, err = s.receiveKeyValue()
	case BinaryType:
		msg.Data, err = s.receiveBinary()
	default:
		return nil, fmt.Errorf("Unknown message type received: %d", op.optype)
	}
	return msg, err
}

func (s *nanoSocket) ReceiveCommand() (command string, err error) {
	var op *Op
	op, err = s.receiveOp()
	if err != nil {
		return
	}
	if op.optype != CommandType {
		err = fmt.Errorf("Expected command message, got %s", op.optype)
		return
	}
	return op.name, nil
}

func (s *nanoSocket) ReceivePostProc() ([]byte, error) {
	op, err := s.receiveOp()
	if err != nil {
		return nil, err
	}
	if op.optype != PostProcType {
		return nil, fmt.Errorf("Expected post-procesing command, got %s", op.optype)
	}
	return s.Recv(0)
}

func (s *nanoSocket) ReceiveKeyValue() (stype storage.DataStoreType, kv *storage.KeyValue, err error) {
	var op *Op
	op, err = s.receiveOp()
	if err != nil {
		return
	}
	if op.optype != KeyValueType {
		err = fmt.Errorf("Expected key value message, got %s", op.optype)
		return
	}
	return s.receiveKeyValue()
}

func (s *nanoSocket) ReceiveBinary() ([]byte, error) {
	op, err := s.receiveOp()
	if err != nil {
		return nil, err
	}
	if op.optype != BinaryType {
		return nil, fmt.Errorf("Expected binary message, got %s", op.optype)
	}
	return s.Recv(0)
}

func (s *nanoSocket) SendCommand(command string) error {
	op := Op{command, CommandType}
	opBytes, err := op.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err := s.Send(opBytes, 0); err != nil {
		return err
	}
	return nil
}

func (s *nanoSocket) SendPostProc(command string, data []byte) error {
	op := Op{command, PostProcType}
	opbytes, err := op.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err := s.Send(opbytes, 0); err != nil {
		return err
	}
	if _, err := s.Send(data, 0); err != nil {
		return err
	}
	return nil
}

func (s *nanoSocket) SendKeyValue(desc string, store storage.DataStoreType, kv *storage.KeyValue) error {
	op := Op{desc, KeyValueType}
	opbytes, err := op.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err := s.Send(opbytes, 0); err != nil {
		return err
	}
	if _, err := s.Send([]byte{byte(store)}, 0); err != nil {
		return err
	}
	if _, err := s.Send(kv.K, 0); err != nil {
		return err
	}
	if _, err := s.Send(kv.V, 0); err != nil {
		return err
	}
	return nil
}

func (s *nanoSocket) SendBinary(desc string, data []byte) error {
	op := Op{desc, BinaryType}
	opbytes, err := op.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err := s.Send(opbytes, 0); err != nil {
		return err
	}
	if _, err := s.Send(data, 0); err != nil {
		return err
	}
	return nil
}
