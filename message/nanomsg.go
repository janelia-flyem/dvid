/*
	Package message supports delivery of DVID commands and key-value pairs
	through a single nanomsg pipeline socket.
*/
package message

import (
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"github.com/janelia-flyem/go/go-nanomsg"
)

const (
	// Default nanomsg REP server is in-process, allowing pushes within a DVID server
	// to create copies.
	DefaultNanomsgAddress = "inproc://a"
)

var (
	registeredOps RegisteredOps
	incoming      *Socket
)

type RegisteredOps struct {
	sync.RWMutex
	callbacks map[string]HandlerFunc
}

type HandlerFunc func(*Socket) error

func init() {
	registeredOps.callbacks = make(map[string]HandlerFunc, 10)
}

func RegisterOpName(name string, f HandlerFunc) {
	registeredOps.Lock()
	registeredOps.callbacks[name] = f
	registeredOps.Unlock()
}

// Establishes a nanomsg pipeline receiver.
func Serve(address string) {
	dvid.Infof("Starting nanomsg receiver on %v\n", address)
	// s, err := nanomsg.NewRepSocket()
	s, err := nanomsg.NewSocket(nanomsg.AF_SP, nanomsg.PULL)
	if err != nil {
		dvid.Criticalf("Unable to create new nanomsg pipeline socket\n")
		return
	}
	_, err = s.Bind(address)
	if err != nil {
		dvid.Criticalf("Unable to connect nanomsg to %q\n", address)
	}
	incoming = &Socket{s}
	for {
		cmd, err := incoming.ReceiveCommand()
		if err != nil {
			dvid.Errorf("Bad receive on nanomsg address %q: %s\n", address, err.Error())
			continue
		}
		callback, found := registeredOps.callbacks[cmd]
		if !found {
			dvid.Errorf("Received unregistered operation: %s\n", cmd)
			continue
		}

		// Handle the operation
		// TODO -- handle concurrent pipeline requests
		if err = callback(incoming); err != nil {
			dvid.Errorf("Error handling op %q: %s\n", cmd, err.Error())
		}
	}
}

func NewPushSocket(target string) (*Socket, error) {
	s, err := nanomsg.NewSocket(nanomsg.AF_SP, nanomsg.PUSH)
	if err != nil {
		return nil, err
	}
	_, err = s.Connect(target)
	if err != nil {
		return nil, err
	}
	return &Socket{s}, nil
}

type OpType uint8

const (
	NotSetType OpType = iota
	CommandType
	BinaryType // hold gobs
	KeyValueType
)

func (optype OpType) String() string {
	switch optype {
	case NotSetType:
		return "not set"
	case CommandType:
		return "command"
	case BinaryType:
		return "binary encoding"
	case KeyValueType:
		return "key value"
	default:
		return "unknown op type"
	}
}

type Op struct {
	name   string
	optype OpType
}

func (op *Op) MarshalBinary() ([]byte, error) {
	if op == nil {
		return nil, nil
	}
	return append([]byte(op.name), byte(op.optype)), nil
}

func (op *Op) UnmarshalBinary(b []byte) error {
	if op == nil {
		return fmt.Errorf("Can't unmarshal into a nil messaging.Op")
	}
	lastI := len(b) - 1
	op.name = string(b[:lastI])
	op.optype = OpType(b[lastI])
	return nil
}

type Socket struct {
	*nanomsg.Socket
}

func (s *Socket) receiveOp() (*Op, error) {
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

func (s *Socket) receiveKeyValue() (stype storage.DataStoreType, kv *storage.KeyValue, err error) {
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

func (s *Socket) receiveBinary() ([]byte, error) {
	return s.Recv(0)
}

// Message handles any kind of data in the message types.
type Message struct {
	Type  OpType
	Name  string
	SType storage.DataStoreType
	KV    *storage.KeyValue
	Data  []byte
}

// ReceiveMessage returns whatever kind of data is sent.
func (s *Socket) ReceiveMessage() (*Message, error) {
	op, err := s.receiveOp()
	if err != nil {
		return nil, err
	}
	msg := &Message{Type: op.optype, Name: op.name}
	switch op.optype {
	case CommandType:
		// nothing needed
	case KeyValueType:
		msg.SType, msg.KV, err = s.receiveKeyValue()
	case BinaryType:
		msg.Data, err = s.receiveBinary()
	default:
		return nil, fmt.Errorf("Unknown message type received: %d", op.optype)
	}
	return msg, err
}

func (s *Socket) ReceiveCommand() (command string, err error) {
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

func (s *Socket) ReceiveKeyValue() (stype storage.DataStoreType, kv *storage.KeyValue, err error) {
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

func (s *Socket) ReceiveBinary() ([]byte, error) {
	op, err := s.receiveOp()
	if err != nil {
		return nil, err
	}
	if op.optype != BinaryType {
		return nil, fmt.Errorf("Expected binary message, got %s", op.optype)
	}
	return s.Recv(0)
}

func (s *Socket) SendCommand(command string) error {
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

func (s *Socket) SendKeyValue(desc string, store storage.DataStoreType, kv *storage.KeyValue) error {
	op := &Op{desc, KeyValueType}
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

func (s *Socket) SendBinary(desc string, b []byte) error {
	op := &Op{desc, BinaryType}
	opbytes, err := op.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err := s.Send(opbytes, 0); err != nil {
		return err
	}
	if _, err := s.Send(b, 0); err != nil {
		return err
	}
	return nil
}
