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

// WriteLog is an append-only log of messages specific to a data instance and UUID.
type WriteLog interface {
	dvid.Store
	Append(dataID, version dvid.UUID, msg LogMessage) error
	CloseLog(dataID, version dvid.UUID) error
}

type ReadLog interface {
	dvid.Store
	ReadAll(dataID, version dvid.UUID) ([]LogMessage, error) // Reads all messages for this instance's version
}

type LogReadable interface {
	GetReadLog() ReadLog
}

type Logable interface {
	GetWriteLog() WriteLog
}
