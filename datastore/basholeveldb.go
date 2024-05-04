//go:build basholeveldb
// +build basholeveldb

package datastore

import (
	_ "github.com/janelia-flyem/dvid/storage/basholeveldb"
	_ "github.com/janelia-flyem/dvid/storage/filelog"
)
