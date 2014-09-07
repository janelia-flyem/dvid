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
	registeredOps RegisteredOps
	incoming      Socket
)

type RegisteredOps struct {
	sync.RWMutex
	commands map[string]CommandFunc
	postproc map[string]PostProcFunc
}

// CommandFunc is a function that handles incoming data from a Socket.
type CommandFunc func(Socket) error

// PostProcFunc is a bridge to functions that perform post-processing after completion
// of a CommandFunc.  For example, we may want to invoke denormalization of transmitted
// normalized data, like the creation of sparse volumes and surfaces and transmission
// of label data.  The []byte payload is typically a serialization of needed parameters.
type PostProcFunc func([]byte) error

func init() {
	registeredOps.commands = make(map[string]CommandFunc, 10)
	registeredOps.postproc = make(map[string]PostProcFunc, 10)
}

func RegisterCommand(name string, f CommandFunc) {
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

type postProcQueue []postProcCommand

func NewPostProcQueue() postProcQueue {
	return make(postProcQueue, 10)
}

func (q postProcQueue) Add(msg *Message) error {
	if msg == nil {
		return fmt.Errorf("Can't add a nil message to a post-processing queue")
	}
	q = append(q, postProcCommand{msg.Name, msg.Data})
	return nil
}

// Runs a queue of post-processing commands, calling functions previously registered
// through RegisterPostProcessing().  If a command has not been registered, it will
// be skipped and noted in log.
func (q postProcQueue) Run() {
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

// Establishes a nanomsg pipeline receiver.
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
	Type  OpType
	Name  string
	SType storage.DataStoreType
	KV    *storage.KeyValue
	Data  []byte
}

type Socket interface {
	ReceiveMessage() (*Message, error)

	ReceivePostProc() ([]byte, error)
	ReceiveCommand() (command string, err error)
	ReceiveKeyValue() (stype storage.DataStoreType, kv *storage.KeyValue, err error)
	ReceiveBinary() ([]byte, error)

	SendCommand(command string) error
	SendPostProc(command string, data []byte) error
	SendKeyValue(desc string, store storage.DataStoreType, kv *storage.KeyValue) error
	SendBinary(desc string, data []byte) error
}
