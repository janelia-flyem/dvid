/*
	Package message supports delivery of DVID commands and key-value pairs
	through a Socket-like connection.
*/
package message

import (
	"fmt"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	registeredOps *RegisteredOps
	sessions      map[string]Session
)

type RegisteredOps struct {
	sync.RWMutex
	commands map[string]NewSessionFunc
	postproc map[string]PostProcFunc
}

// Session has a state and processes incoming messages via a finite state machine.
type Session interface {
	ProcessMessage(*Message) error
}

// NewSessionFunc is a function that creates new sessions
type NewSessionFunc func(*Message) (Session, error)

// PostProcFunc is a bridge to functions that perform post-processing after completion
// of a CommandFunc.  For example, we may want to invoke denormalization of transmitted
// normalized data, like the creation of sparse volumes and surfaces and transmission
// of label data.  The []byte payload is typically a serialization of needed parameters.
type PostProcFunc func([]byte) error

func init() {
	registeredOps = &RegisteredOps{
		commands: make(map[string]NewSessionFunc),
		postproc: make(map[string]PostProcFunc),
	}
	sessions = make(map[string]Session)
}

func RegisterCommand(name string, f NewSessionFunc) {
	registeredOps.Lock()
	registeredOps.commands[name] = f
	registeredOps.Unlock()
}

func RegisterPostProcessing(name string, f PostProcFunc) {
	registeredOps.Lock()
	registeredOps.postproc[name] = f
	registeredOps.Unlock()
}

type postProcCommand struct {
	name string
	data []byte
}

type PostProcQueue []postProcCommand

func NewPostProcQueue() PostProcQueue {
	return PostProcQueue{}
}

func (q PostProcQueue) Add(msg *Message) error {
	if msg == nil {
		return fmt.Errorf("Can't add a nil message to a post-processing queue")
	}
	q = append(q, postProcCommand{msg.Name, msg.Data})
	return nil
}

// Runs a queue of post-processing commands, calling functions previously registered
// through RegisterPostProcessing().  If a command has not been registered, it will
// be skipped and noted in log.
func (q PostProcQueue) Run() {
	for _, command := range q {
		callback, found := registeredOps.postproc[command.name]
		if !found {
			dvid.Errorf("Skipping unregistered post-processing command %q\n", command.name)
			continue
		}
		if err := callback(command.data); err != nil {
			dvid.Errorf("Error in post-proc command %q: %s\n", command.data, err.Error())
		}
	}
}

// OpTYpe specifies the type of message being transmitted.
type OpType uint8

const (
	NotSetType   OpType = iota
	CommandType         // free-form strings
	PostProcType        // post-processing to be done at end of command processing
	BinaryType          // hold gobs
	KeyValueType
)

func (optype OpType) String() string {
	switch optype {
	case NotSetType:
		return "not set"
	case CommandType:
		return "command"
	case PostProcType:
		return "post-processing command"
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

// Message handles any kind of data in the message types.
type Message struct {
	Type    OpType
	Name    string
	Session string
	SType   storage.DataStoreType
	KV      *storage.KeyValue
	Data    []byte
}

type Socket interface {
	SendCommand(command string) error
	SendPostProc(command string, data []byte) error
	SendKeyValue(desc string, store storage.DataStoreType, kv *storage.KeyValue) error
	SendBinary(desc string, data []byte) error
}
