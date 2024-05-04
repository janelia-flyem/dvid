//go:build badger
// +build badger

package datastore

import (
	_ "github.com/janelia-flyem/dvid/storage/badger"
	_ "github.com/janelia-flyem/dvid/storage/filelog"
)
