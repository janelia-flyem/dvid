// +build !clustered,!gcloud

package storage

import (
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
)

type engineManager struct {
	// True if SetupEngines() has been called
	setup bool

	// Cached type-asserted interfaces
	kvEngine    Engine
	kvDB        OrderedKeyValueDB
	kvSetter    OrderedKeyValueSetter
	kvGetter    OrderedKeyValueGetter
	graphEngine Engine
	graphDB     GraphDB
	graphSetter GraphSetter
	graphGetter GraphGetter
}

// Engines provides access to key-value, ordered key-value, and graph datastores.
var Engines engineManager

func (m *engineManager) KeyValueDB() KeyValueDB {
	return m.kvDB
}

func (m *engineManager) KeyValueGetter() KeyValueGetter {
	return m.kvGetter
}

func (m *engineManager) KeyValueSetter() KeyValueSetter {
	return m.kvSetter
}

func (m *engineManager) KeyValueBatcher() KeyValueBatcher {
	batcher, ok := m.kvSetter.(KeyValueBatcher)
	if !ok {
		return nil
	}
	return batcher
}

func (m *engineManager) OrderedKeyValueDB() OrderedKeyValueDB {
	return m.kvDB
}

func (m *engineManager) OrderedKeyValueGetter() OrderedKeyValueGetter {
	return m.kvGetter
}

func (m *engineManager) OrderedKeyValueSetter() OrderedKeyValueSetter {
	return m.kvSetter
}

func (m *engineManager) GraphDB() GraphDB {
	return m.graphDB
}

func (m *engineManager) GraphGetter() GraphGetter {
	return m.graphGetter
}

func (m *engineManager) GraphSetter() GraphSetter {
	return m.graphSetter
}

func SetupEngines(path string, config dvid.Config) error {
	var err error
	var ok bool

	create := true
	Engines.kvEngine, err = NewKeyValueStore(path, create, config)
	if err != nil {
		return err
	}
	Engines.kvDB, ok = Engines.kvEngine.(OrderedKeyValueDB)
	if !ok {
		return fmt.Errorf("Database at %v is not a valid ordered key-value database", path)
	}
	Engines.kvSetter, ok = Engines.kvEngine.(OrderedKeyValueSetter)
	if !ok {
		return fmt.Errorf("Database at %v is not a valid ordered key-value setter", path)
	}
	Engines.kvGetter, ok = Engines.kvEngine.(OrderedKeyValueGetter)
	if !ok {
		return fmt.Errorf("Database at %v is not a valid ordered key-value getter", path)
	}

	Engines.graphEngine, err = NewGraphStore(path, create, config, kvdb)
	if err != nil {
		return err
	}
	Engines.graphDB, ok = Engines.graphEngine.(GraphDB)
	if !ok {
		return fmt.Errorf("Database at %v cannot support a graph database", path)
	}
	Engines.graphSetter, ok = Engines.graphEngine.(GraphSetter)
	if !ok {
		return fmt.Errorf("Database at %v cannot support a graph setter", path)
	}
	Engines.graphGetter, ok = Engines.graphEngine.(GraphGetter)
	if !ok {
		return fmt.Errorf("Database at %v cannot support a graph getter", path)
	}

	Engines.setup = true
}

// --- Implement the three tiers of storage.
// --- In the case of a single local server with embedded storage engines, it's simpler
// --- because we don't worry about cross-process synchronization.

func SetupTiers() {
	MetaData = metaData{Engines.kvDB}
	SmallData = smallData{Engines.kvDB}
	BigData = bigData{Engines.kvDB}
}
