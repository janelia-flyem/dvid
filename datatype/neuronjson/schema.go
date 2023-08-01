package neuronjson

import (
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// Metadata key-value support (all non-neuron annotations)

// load metadata from storage
func (d *Data) loadMetadata(ctx storage.VersionedCtx, meta Schema) (val []byte, err error) {
	var tkey storage.TKey
	if tkey, err = getMetadataKey(meta); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	var byteVal []byte
	if byteVal, err = db.Get(ctx, tkey); err != nil {
		return
	}
	return byteVal, nil
}

// gets metadata from either in-memory db if HEAD or from store
func (d *Data) getMetadata(ctx storage.VersionedCtx, meta Schema) (val []byte, err error) {
	if ctx.Head() {
		d.metadataMu.RLock()
		defer d.metadataMu.RUnlock()
		if val, found := d.metadata[meta]; found {
			return val, nil
		} else {
			return nil, nil
		}
	}
	return d.loadMetadata(ctx, meta)
}

// get fully compiled JSON schema for use -- TODO
func (d *Data) getJSONSchema(ctx storage.VersionedCtx) (sch *jsonschema.Schema, err error) {
	if ctx.Head() {
		d.metadataMu.RLock()
		sch = d.compiledSchema
		d.metadataMu.RUnlock()
		if sch != nil {
			return
		}
	}

	var tkey storage.TKey
	if tkey, err = getMetadataKey(JSONSchema); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	var byteVal []byte
	if byteVal, err = db.Get(ctx, tkey); err != nil {
		return
	}
	if len(byteVal) == 0 {
		return nil, fmt.Errorf("no JSON Schema available")
	}
	if ctx.Head() {
		d.metadataMu.RLock()
		d.metadata[JSONSchema] = byteVal
		d.metadataMu.RUnlock()
	}

	sch, err = jsonschema.CompileString("schema.json", string(byteVal))
	if err != nil {
		return
	}
	if sch == nil {
		return nil, fmt.Errorf("no JSON Schema available")
	}
	return
}

func (d *Data) putMetadata(ctx storage.VersionedCtx, val []byte, meta Schema) (err error) {
	var tkey storage.TKey
	if tkey, err = getMetadataKey(meta); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	if err = db.Put(ctx, tkey, val); err != nil {
		return
	}

	// If we could persist metadata, add it to in-memory db if head.
	if ctx.Head() {
		d.metadataMu.Lock()
		d.metadata[meta] = val
		if meta == JSONSchema {
			d.compiledSchema, err = jsonschema.CompileString("schema.json", string(val))
			if err != nil {
				d.compiledSchema = nil
				dvid.Errorf("Unable to compile json schema: %v\n", err)
			}
		}
		d.metadataMu.Unlock()
	}
	return nil
}

func (d *Data) metadataExists(ctx storage.VersionedCtx, meta Schema) (exists bool, err error) {
	if ctx.Head() {
		d.metadataMu.RLock()
		defer d.metadataMu.RUnlock()
		_, found := d.metadata[meta]
		return found, nil
	}
	var tkey storage.TKey
	if tkey, err = getMetadataKey(meta); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	return db.Exists(ctx, tkey)
}

func (d *Data) deleteMetadata(ctx storage.VersionedCtx, meta Schema) (err error) {
	var tkey storage.TKey
	if tkey, err = getMetadataKey(meta); err != nil {
		return
	}
	var db storage.KeyValueDB
	if db, err = datastore.GetKeyValueDB(d); err != nil {
		return
	}
	if err = db.Delete(ctx, tkey); err != nil {
		return
	}
	if ctx.Head() {
		d.metadataMu.Lock()
		defer d.metadataMu.Unlock()
		delete(d.metadata, meta)
	}
	return nil
}
