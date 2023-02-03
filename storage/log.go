package storage

import (
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
)

// ErrLogEOF is an error returned by a log Read() on an end of file
var ErrLogEOF = fmt.Errorf("log EOF")

// LogMessage is a single message to a log
type LogMessage struct {
	EntryType uint16
	Data      []byte
}

// WriteLog is an append-only log of messages specific to a topic or a data instance and UUID.
type WriteLog interface {
	dvid.Store
	Append(dataID, version dvid.UUID, msg LogMessage) error
	CloseLog(dataID, version dvid.UUID) error
	TopicAppend(topic string, msg LogMessage) error
	TopicClose(topic string) error
}

type ReadLog interface {
	dvid.Store
	ReadBinary(dataID, version dvid.UUID) ([]byte, error)
	ReadAll(dataID, version dvid.UUID) ([]LogMessage, error)
	StreamAll(dataID, version dvid.UUID, ch chan LogMessage) error
}

type LogReadable interface {
	ReadLogRequired() bool
	GetReadLog() ReadLog
}

type LogWritable interface {
	WriteLogRequired() bool
	GetWriteLog() WriteLog
}

type Logable interface {
	LogReadable
	LogWritable
}
