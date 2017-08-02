package storage

import "github.com/janelia-flyem/dvid/dvid"

// Log is an append-only log of messages specific to a data instance and UUID.
type WriteLog interface {
	dvid.Store
	Append(entryType uint16, dataID, version dvid.UUID, data []byte) error
}

type ReadLog interface {
	dvid.Store
	Read(dataID, version dvid.UUID) (entryType uint16, data []byte, err error)
}

type Logable interface {
	GetWriteLog() WriteLog
}
