// +build bigtable

// It doesn't use tombstones when deleting
package bigtable

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
    
	"github.com/janelia-flyem/go/semver"

	"golang.org/x/net/context"
	"google.golang.org/cloud"
	api "google.golang.org/cloud/bigtable"
	"google.golang.org/cloud/bigtable/bttest"
	"google.golang.org/grpc"
)

const familyName = "versions"

//initialized by newBigTable.
var (
	client *api.Client
	tbl    *api.Table
)

func init() {
	ver, err := semver.Make("0.1.0")
	if err != nil {
		dvid.Errorf("Unable to make semver in bigtable: %v\n", err)
	}
	e := Engine{"bigtable", "Google's Cloud BigTable", ver}
	storage.RegisterEngine(e)
}

// --- Engine Implementation ------

type Engine struct {
	name   string
	desc   string
	semver semver.Version
}

func (e Engine) GetName() string {
	return e.name
}

func (e Engine) GetDescription() string {
	return e.desc
}

func (e Engine) GetSemVer() semver.Version {
	return e.semver
}

func (e Engine) String() string {
	return fmt.Sprintf("%s [%s]", e.name, e.semver)
}

// NewStore returns a leveldb suitable for mutable storage.
// The passed Config must contain:
// "project-id" string  ex: "janelia-flyem-project"
// "zone" string ex: "us-central1-b"
// "cluster" string ex: "dvid-cluster"
// "table" string ex: "mutable"
// BigTable cluster can't be created from the go API
// to create a cluster read https://cloud.google.com/bigtable/docs/creating-cluster
func (e Engine) NewStore(config dvid.StoreConfig) (dvid.Store, bool, error) {
	return e.newBigTable(config)
}

func parseConfig(config dvid.StoreConfig) (*BigTable, error) {
    c := config.GetAll()
    
    v, found := c["project"]
    if !found {
        return nil, fmt.Errorf("%q must be specified for BigTable configuration", "project")
    }
    project, ok := v.(string)
    if !ok {
        return nil, fmt.Errorf("%q setting must be a string (%v)", "project", v)
    }

    v, found = c["zone"]
    if !found {
        return nil, fmt.Errorf("%q must be specified for BigTable configuration", "zone")
    }
    zone, ok := v.(string)
    if !ok {
        return nil, fmt.Errorf("%q setting must be a string (%v)", "zone", v)
    }

    v, found = c["cluster"]
    if !found {
        return nil, fmt.Errorf("%q must be specified for BigTable configuration", "cluster")
    }
    cluster, ok := v.(string)
    if !ok {
        return nil, fmt.Errorf("%q setting must be a string (%v)", "cluster", v)
    }

    v, found = c["table"]
    if !found {
        return nil, fmt.Errorf("%q must be specified for BigTable configuration", "table")
    }
    table, ok := v.(string)
    if !ok {
        return nil, fmt.Errorf("%q setting must be a string (%v)", "table", v)
    }

	var testing bool
    v, found = c["testing"]
    if !found {
		testing, ok = v.(bool)
		if !ok {
			return nil, fmt.Errorf("%q setting must be a bool (%v)", "testing", v)
		}
    }

	bt := &BigTable{
		project: project,
		zone:    zone,
		cluster: cluster,
		table:   table,
		testing: testing,
		ctx:     context.Background(),
	}

	return bt, nil
}

// TODO -- Work on testable BigTable implementation.
/*
func (e Engine) GetTestConfig() (*Backend, error) {
    tc := map[string]interface{} {
        "project": "project",
        "zone":    "zone",
        "cluster": "cluster",
        "table":   fmt.Sprintf("dvid-test-%x", uuid.NewV4().Bytes()),
        "testing": true,
    }
    var c dvid.Config 
    c.SetAll(tc) 
	testConfig := map[string]dvid.StoreConfig{
        "default": &dvid.StoreConfig{Config: c, Engine: "bigtable"},
    }
    return testConfig, nil
}
*/

func NewAdminClient(bt *BigTable) (adminClient *api.AdminClient, err error) {

	if bt.testing {

		testConn, err := grpc.Dial(bt.testSrv.Addr, grpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("Unable to create bigTable local test server. %v", err)
		}
		//Connects to a local bigtable with no security;
		// The project, zone, cluster values are ignored.
		adminClient, err = api.NewAdminClient(bt.ctx, bt.project, bt.zone, bt.cluster, cloud.WithBaseGRPC(testConn))

	} else {

		//Uses Application Default Credentials to authenticate into Google's Cloud.
		adminClient, err = api.NewAdminClient(bt.ctx, bt.project, bt.zone, bt.cluster)
	}
	if err != nil {
		return nil, fmt.Errorf("Unable to create a table admin client. %v", err)
	}

	return
}

func NewClient(bt *BigTable) (client *api.Client, err error) {
	if bt.testing {
		testSrv, err := bttest.NewServer() //TODO close the testSrv if neccesary
		if err != nil {
			return nil, fmt.Errorf("Unable to create bigTable local test server. %v", err)
		}
		testConn, err := grpc.Dial(testSrv.Addr, grpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("Unable to create bigTable local test server. %v", err)
		}
		//Connects to a local bigtable.
		//It with no security; The project, zone, cluster values are ignored.
		client, err = api.NewClient(bt.ctx, bt.project, bt.zone, bt.cluster, cloud.WithBaseGRPC(testConn))
	} else {
		//Uses Application Default Credentials to authenticate into Google's Cloud.
		client, err = api.NewClient(bt.ctx, bt.project, bt.zone, bt.cluster)
	}
	if err != nil {
		return nil, fmt.Errorf("Unable to create a table client. %v", err)
	}
	return
}

// Set up admin client, tables, and column families.
func (e *Engine) newBigTable(config dvid.StoreConfig) (*BigTable, bool, error) {
	bt, err := parseConfig(config)
	if err != nil {
		return nil, false, fmt.Errorf("Error in newBigTable() %s\n", err)
	}

	if bt.testing {

	}

	// Set up admin client, tables, and column families.
	// NewAdminClient uses Application Default Credentials to authenticate.

	adminClient, err := NewAdminClient(bt)
	if err != nil {
		return nil, false, fmt.Errorf("Unable to create a table admin client. %v", err)
	}
	tables, err := adminClient.Tables(bt.ctx)
	if err != nil {
		return nil, false, fmt.Errorf("Unable to fetch table list. %v", err)
	}

	var created bool
	if !sliceContains(tables, bt.table) {
		if err := adminClient.CreateTable(bt.ctx, bt.table); err != nil {
			return nil, false, fmt.Errorf("Unable to create table: %v. %v", bt.table, err)

		}
		created = true
	}

	tblInfo, err := adminClient.TableInfo(bt.ctx, bt.table)
	if err != nil {
		dvid.Errorf("Unable to read info for table: %v. %v", bt.table, err)
		return nil, created, err
	}

	if !sliceContains(tblInfo.Families, familyName) {
		if err := adminClient.CreateColumnFamily(bt.ctx, bt.table, familyName); err != nil {
			dvid.Errorf("Unable to create column family: %v. %v", familyName, err)
			return nil, created, err
		}
	}
	adminClient.Close()

	// Set up Bigtable data operations client.
	// NewClient uses Application Default Credentials to authenticate.
	client, err = NewClient(bt)
	if err != nil {
		return nil, created, err
	}

	tbl = client.Open(bt.table)

	return bt, created, nil
}

func (e Engine) Delete(config dvid.StoreConfig) error {

	bt, err := parseConfig(config)
	if err != nil {
		return fmt.Errorf("Error in Delete() %s\n", err)
	}

	// Set up admin client, tables, and column families.
	// NewAdminClient uses Application Default Credentials to authenticate.
	adminClient, err := api.NewAdminClient(bt.ctx, bt.project, bt.zone, bt.cluster)
	if err != nil {
		return fmt.Errorf("Unable to create a table admin client. %v", err)
	}

	err = adminClient.DeleteTable(bt.ctx, bt.table)
	if err != nil {
		return fmt.Errorf("Unable to delete table: %v", err)
	}

	return err
}

type BigTable struct {
	project string
	zone    string
	cluster string
	table   string
	testing bool
	testSrv *bttest.Server
	ctx     context.Context
}

func (db *BigTable) String() string {
	return fmt.Sprintf("google bigtable, project %s, table %s", db.project, db.table)
}

// Get returns a value given a key.
func (db *BigTable) Get(ctx storage.Context, tk storage.TKey) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call Get() on nil BigTable")
	}
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in Get()")
	}

	unvKey, verKey, err := ctx.SplitKey(tk)
	if err != nil {
		dvid.Errorf("Error in Get(): %v\n", err)
	}

	r, err := tbl.ReadRow(db.ctx, encodeKey(unvKey))

	//A missing row will return a zero-length map and a nil error
	if len(r) == 0 {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	value, err := getValue(r, verKey)
	if err != nil {
		return nil, err
	}

	return value, nil
}

// GetRange returns a range of values spanning (TkBeg, kEnd) keys.
func (db *BigTable) GetRange(ctx storage.Context, TkBeg, TkEnd storage.TKey) ([]*storage.TKeyValue, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call GetRange() on nil BigTable")
	}
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in GetRange()")
	}

	unvKeyBeg, _, err := ctx.SplitKey(TkBeg)
	if err != nil {
		dvid.Errorf("Error in GetRange(): %v\n", err)
	}
	unvKeyEnd, _, err := ctx.SplitKey(TkEnd)
	if err != nil {
		dvid.Errorf("Error in GetRange(): %v\n", err)
	}

	tKeyValues := make([]*storage.TKeyValue, 0)

	rr := api.NewRange(encodeKey(unvKeyBeg), encodeKey(unvKeyEnd))
	err = tbl.ReadRows(db.ctx, rr, func(r api.Row) bool {

		unvKeyRow, err := decodeKey(r.Key())
		if err != nil {
			dvid.Errorf("Error in GetRange() decodeKey(r.Key()): %v\n", err)
			return false
		}

		// dvid.Infof("GetRange() with row key= %v", r.Key())
		for _, readItem := range r[familyName] {

			verKey, err := decodeKey(readItem.Column)
			if err != nil {
				dvid.Errorf("Error in GetRange() decodeKey(readItem.Column): %v\n", err)
				return false
			}

			fullKey := storage.MergeKey(unvKeyRow, verKey)
			// dvid.Infof("colum key= %v , timestamp = %v", verKey, readItem.Timestamp)
			tkey, err := storage.TKeyFromKey(fullKey)
			if err != nil {
				dvid.Errorf("Error in GetRange() storage.TKeyFromKey(fullKey): %v\n", err)
				return false
			}

			kv := storage.TKeyValue{tkey, readItem.Value}
			tKeyValues = append(tKeyValues, &kv)
		}

		return true // keep going
	})

	return tKeyValues, err
}

// KeysInRange returns a range of type-specific key components spanning (TkBeg, TkEnd).
func (db *BigTable) KeysInRange(ctx storage.Context, TkBeg, TkEnd storage.TKey) ([]storage.TKey, error) {
	if db == nil {
		return nil, fmt.Errorf("Can't call KeysInRange() on nil BigTable")
	}
	if ctx == nil {
		return nil, fmt.Errorf("Received nil context in KeysInRange()")
	}

	tKeys := make([]storage.TKey, 0)

	unvKeyBeg, _, err := ctx.SplitKey(TkBeg)
	if err != nil {
		dvid.Errorf("Error in KeysInRange(): %v\n", err)
	}

	unvKeyEnd, _, err := ctx.SplitKey(TkEnd)
	if err != nil {
		dvid.Errorf("Error in KeysInRange(): %v\n", err)
	}

	rr := api.NewRange(encodeKey(unvKeyBeg), encodeKey(unvKeyEnd))

	err = tbl.ReadRows(db.ctx, rr, func(r api.Row) bool {

		if len(r[familyName]) == 0 {
			dvid.Errorf("Error in KeysInRange(): row has no columns")
			return false
		}

		unvKeyRow, err := decodeKey(r.Key())
		if err != nil {
			dvid.Errorf("Error in KeysInRange(): %v\n", err)
			return false
		}

		verKeyRow, err := decodeKey(r[familyName][0].Column)
		if err != nil {
			dvid.Errorf("Error in KeysInRange(): %v\n", err)
			return false
		}

		fullKey := storage.MergeKey(unvKeyRow, verKeyRow)
		tkey, err := storage.TKeyFromKey(fullKey)
		if err != nil {
			dvid.Errorf("Error in KeysInRange(): %v\n", err)
			return false
		}

		tKeys = append(tKeys, tkey)

		return true // keep going
	}, api.RowFilter(api.StripValueFilter()))

	return tKeys, err
}

// SendKeysInRange sends a range of full keys down a key channel.
func (db *BigTable) SendKeysInRange(ctx storage.Context, TkBeg, TkEnd storage.TKey, ch storage.KeyChan) error {
	if db == nil {
		return fmt.Errorf("Can't call SendKeysInRange() on nil BigTable")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in SendKeysInRange()")
	}

	unvKeyBeg, _, err := ctx.SplitKey(TkBeg)
	if err != nil {
		dvid.Errorf("Error in SendKeysInRange(): %v\n", err)
	}

	unvKeyEnd, _, err := ctx.SplitKey(TkEnd)
	if err != nil {
		dvid.Errorf("Error in SendKeysInRange(): %v\n", err)
	}

	rr := api.NewRange(encodeKey(unvKeyBeg), encodeKey(unvKeyEnd))

	err = tbl.ReadRows(db.ctx, rr, func(r api.Row) bool {

		unvKeyRow, err := decodeKey(r.Key())
		if err != nil {
			dvid.Errorf("Error in SendKeysInRange(): %v\n", err)
			return false
		}

		//I need the versioned key to merged it with the unversioned
		// and send it throu the channel
		for _, readItem := range r[familyName] {

			verKey, err := decodeKey(readItem.Column)
			if err != nil {
				dvid.Errorf("Error in SendKeysInRange(): %v\n", err)
				return false
			}

			fullKey := storage.MergeKey(unvKeyRow, verKey)

			ch <- fullKey
		}

		return true // keep going
	}, api.RowFilter(api.StripValueFilter()))

	return err
}

// ProcessRange sends a range of type key-value pairs to type-specific chunk handlers,
// allowing chunk processing to be concurrent with key-value sequential reads.
// Since the chunks are typically sent during sequential read iteration, the
// receiving function can be organized as a pool of chunk handling goroutines.
// See datatype/imageblk.ProcessChunk() for an example.
func (db *BigTable) ProcessRange(ctx storage.Context, TkBeg, TkEnd storage.TKey, op *storage.ChunkOp, f storage.ChunkFunc) error {
	if db == nil {
		return fmt.Errorf("Can't call ProcessRange() on nil BigTable")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in ProcessRange()")
	}

	unvKeyBeg, verKey, err := ctx.SplitKey(TkBeg)
	if err != nil {
		dvid.Errorf("Error in ProcessRange(): %v\n", err)
	}

	unvKeyEnd, _, err := ctx.SplitKey(TkEnd)
	if err != nil {
		dvid.Errorf("Error in ProcessRange(): %v\n", err)
	}

	rr := api.NewRange(encodeKey(unvKeyBeg), encodeKey(unvKeyEnd))
	err = tbl.ReadRows(db.ctx, rr, func(r api.Row) bool {

		if len(r[familyName]) == 0 {
			dvid.Errorf("Error in KeysInRange(): row has no columns")
			return false
		}

		unvKeyRow, err := decodeKey(r.Key())
		if err != nil {
			dvid.Errorf("Error in ProcessRange(): %v\n", err)
			return false
		}

		verKeyRow, err := decodeKey(r[familyName][0].Column)
		if err != nil {
			dvid.Errorf("Error in ProcessRange(): %v\n", err)
			return false
		}

		fullKey := storage.MergeKey(unvKeyRow, verKeyRow)
		tkey, err := storage.TKeyFromKey(fullKey)
		if err != nil {
			dvid.Errorf("Error in ProcessRange(): %v\n", err)
			return false
		}

		if op.Wg != nil {
			op.Wg.Add(1)
		}
		value, err := getValue(r, verKey)
		if err != nil {
			dvid.Errorf("Error in ProcessRange(): %v\n", err)
			return false
		}

		tkv := storage.TKeyValue{tkey, value}
		chunk := &storage.Chunk{op, &tkv}
		if err := f(chunk); err != nil {
			dvid.Errorf("Error in ProcessRange(): %v\n", err)
			return false
		}

		return true // keep going
	})

	return err
}

// RawRangeQuery sends a range of full keys.  This is to be used for low-level data
// retrieval like DVID-to-DVID communication and should not be used by data type
// implementations if possible because each version's key-value pairs are sent
// without filtering by the current version and its ancestor graph.  A nil is sent
// down the channel when the range is complete.
func (db *BigTable) RawRangeQuery(kStart, kEnd storage.Key, keysOnly bool, out chan *storage.KeyValue) error {
	if db == nil {
		return fmt.Errorf("Can't call RawRangeQuery() on nil BigTable")
	}

	unvKeyBeg, verKeyBeg, err := storage.SplitKey(kStart)
	if err != nil {
		dvid.Errorf("Error in RawRangeQuery(): %v\n", err)
		return err
	}

	unvKeyEnd, verKeyEnd, err := storage.SplitKey(kEnd)
	if err != nil {
		dvid.Errorf("Error in RawRangeQuery(): %v\n", err)
		return err
	}

	rr := api.NewRange(encodeKey(unvKeyBeg), encodeKey(unvKeyEnd))

	err = tbl.ReadRows(db.ctx, rr, func(r api.Row) bool {

		unvKeyRow, err := decodeKey(r.Key())
		if err != nil {
			dvid.Errorf("Error in RawRangeQuery(): %v\n", err)
			return false
		}

		//I need the versioned key to merged it with the unversioned
		// and send it throu the channel
		for _, readItem := range r[familyName] {

			verKey, err := decodeKey(readItem.Column)
			if err != nil {
				fmt.Errorf("Error in RawRangeQuery(): %v\n", err)
				return false
			}

			lowerLimit := bytes.Equal(unvKeyBeg, unvKeyRow) && bytes.Compare(verKey, verKeyBeg) == -1
			upperLimit := bytes.Equal(unvKeyEnd, unvKeyRow) && bytes.Compare(verKey, verKeyEnd) >= 0

			if lowerLimit || upperLimit {
				continue
			}

			fullKey := storage.MergeKey(unvKeyRow, verKey)

			kv := storage.KeyValue{fullKey, readItem.Value}

			out <- &kv
		}

		return true // keep going
	})
	return nil
}

// Put writes a value with given key in a possibly versioned context.
func (db *BigTable) Put(ctx storage.Context, tkey storage.TKey, value []byte) error {
	if db == nil {
		return fmt.Errorf("Can't call Put() on nil BigTable")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in Put()")
	}

	unvKey, verKey, err := ctx.SplitKey(tkey)
	if err != nil {
		dvid.Errorf("Error in Put(): %v\n", err)
	}

	mut := api.NewMutation()

	// dvid.Infof("Putting value %s\n", string(value))
	mut.Set(familyName, encodeKey(verKey), 0, value)
	err = tbl.Apply(db.ctx, encodeKey(unvKey), mut)
	if err != nil {
		return fmt.Errorf("Error in Put(): %v\n", err)
	}

	return err
}

// Delete deletes a key-value pair so that subsequent Get on the key returns nil.
func (db *BigTable) Delete(ctx storage.Context, tkey storage.TKey) error {
	if db == nil {
		return fmt.Errorf("Can't call Delete() on nil BigTable")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in Delete()")
	}

	unvKey, verKey, err := ctx.SplitKey(tkey)
	if err != nil {
		dvid.Errorf("Error in Delete(): %v\n", err)
	}

	r, err := tbl.ReadRow(db.ctx, encodeKey(unvKey), api.RowFilter(api.StripValueFilter()))

	//A missing row will return a zero-length map and a nil error
	if len(r) == 0 {
		return fmt.Errorf("Error in Delete(): This unvKey doesn't exists")
	}

	if err != nil {
		return err
	}
	if len(r[familyName]) == 0 {
		return fmt.Errorf("Error in Delete(): This row is empty")
	}
	_, err = getValue(r, verKey)
	if err != nil {
		return fmt.Errorf("Error in Delete(): The version to be deleted doesn't exist")
	}

	mut := api.NewMutation()

	//There is only one version left, and is the one we are trying to delete\
	//remove the whole row
	if len(r[familyName]) == 1 {
		mut.DeleteRow()
	} else {
		mut.DeleteCellsInColumn(familyName, encodeKey(verKey))
	}

	err = tbl.Apply(db.ctx, encodeKey(unvKey), mut)
	if err != nil {
		return fmt.Errorf("Error in Delete(): %v\n", err)
	}

	return err
}

// RawPut is a low-level function that puts a key-value pair using full keys.
// This can be used in conjunction with RawRangeQuery.
func (db *BigTable) RawPut(fullKey storage.Key, value []byte) error {
	if db == nil {
		return fmt.Errorf("Can't call RawPut() on nil BigTable")
	}

	unvKey, verKey, err := storage.SplitKey(fullKey)
	if err != nil {
		return fmt.Errorf("Error in RawPut(): %v\n", err)
	}

	mut := api.NewMutation()

	mut.Set(familyName, encodeKey(verKey), 0, value)
	err = tbl.Apply(db.ctx, encodeKey(unvKey), mut)
	if err != nil {
		return fmt.Errorf("Error in RawPut(): %v\n", err)
	}

	return err
}

// RawDelete is a low-level function.  It deletes a key-value pair using full keys
// without any context.  This can be used in conjunction with RawRangeQuery.
func (db *BigTable) RawDelete(fullKey storage.Key) error {
	if db == nil {
		return fmt.Errorf("Can't call RawDelete() on nil BigTable")
	}

	unvKey, verKey, err := storage.SplitKey(fullKey)
	if err != nil {
		return fmt.Errorf("Error in RawDelete(): %v\n", err)
	}

	r, err := tbl.ReadRow(db.ctx, encodeKey(unvKey), api.RowFilter(api.StripValueFilter()))

	//A missing row will return a zero-length map and a nil error
	if len(r) == 0 {
		return fmt.Errorf("Error in RawDelete(): This unvKey doesn't exists")
	}

	if err != nil {
		return err
	}
	if len(r[familyName]) == 0 {
		return fmt.Errorf("Error in Delete(): This row is empty")
	}
	_, err = getValue(r, verKey)
	if err != nil {
		return fmt.Errorf("Error in Delete(): The version to be deleted doesn't exist")
	}

	mut := api.NewMutation()

	//There is only one version left, and is the one we are trying to delete\
	//remove the whole row
	if len(r[familyName]) == 1 {
		mut.DeleteRow()
	} else {
		mut.DeleteCellsInColumn(familyName, encodeKey(verKey))
	}

	err = tbl.Apply(db.ctx, encodeKey(unvKey), mut)
	if err != nil {
		return fmt.Errorf("Error in Delete(): %v\n", err)
	}

	return err
}

// Put key-value pairs.  Note that it could be more efficient to use the Batcher
// interface so you don't have to create and keep a slice of KeyValue.  Some
// databases like leveldb will copy on batch put anyway.
func (db *BigTable) PutRange(ctx storage.Context, TKeyValue []storage.TKeyValue) error {
	if db == nil {
		return fmt.Errorf("Can't call PutRange() on nil BigTable")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in PutRange()")
	}

	for _, tkeyvalue := range TKeyValue {

		unvKey, verKey, err := ctx.SplitKey(tkeyvalue.K)
		if err != nil {
			dvid.Errorf("Error in PutRange(): %v\n", err)
		}

		mut := api.NewMutation()

		mut.Set(familyName, encodeKey(verKey), 0, tkeyvalue.V)
		err = tbl.Apply(db.ctx, encodeKey(unvKey), mut)
		if err != nil {
			dvid.Errorf("Failed to Put value in PutRange()")
		}
	}

	return nil
}

// DeleteRange removes all key-value pairs with keys in the given range.
// For all versions
func (db *BigTable) DeleteRange(ctx storage.Context, TkBeg, TkEnd storage.TKey) error {
	if db == nil {
		return fmt.Errorf("Can't call DeleteRange() on nil BigTable")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteRange()")
	}

	unvKeyBeg, _, err := ctx.SplitKey(TkBeg)
	if err != nil {
		return fmt.Errorf("Error in DeleteRange(): %v\n", err)
	}
	unvKeyEnd, _, err := ctx.SplitKey(TkEnd)
	if err != nil {
		return fmt.Errorf("Error in DeleteRange(): %v\n", err)
	}

	rr := api.NewRange(encodeKey(unvKeyBeg), encodeKey(unvKeyEnd))

	err = tbl.ReadRows(db.ctx, rr, func(r api.Row) bool {

		unvKeyRow, err := decodeKey(r.Key())
		if err != nil {
			dvid.Errorf("Error in DeleteRange(): %v\n", err)
			return false
		}

		mut := api.NewMutation()
		mut.DeleteRow()
		err = tbl.Apply(db.ctx, encodeKey(unvKeyRow), mut)
		if err != nil {
			dvid.Errorf("Failed to delete row in DeleteRange()")
		}

		return true // keep going
	}, api.RowFilter(api.StripValueFilter()))

	return err
}

// DeleteAll removes all key-value pairs for the context.  If allVersions is true,
// then all versions of the data instance are deleted.
func (db *BigTable) DeleteAll(ctx storage.Context, allVersions bool) error {
	if db == nil {
		return fmt.Errorf("Can't call DeleteAll() on nil BigTable")
	}
	if ctx == nil {
		return fmt.Errorf("Received nil context in DeleteAll()")
	}

	//Row range corresponde to all keys corresponding to this data instace.
	min, max := ctx.KeyRange()
	rr := api.NewRange(encodeKey(min), encodeKey(max))

	err := tbl.ReadRows(db.ctx, rr, func(r api.Row) bool {

		unvKeyRow, err := decodeKey(r.Key())
		if err != nil {
			dvid.Errorf("Error in DeleteAll(): %v\n", err)
			return false
		}

		if allVersions {

			mut := api.NewMutation()
			mut.DeleteRow()
			err := tbl.Apply(db.ctx, encodeKey(unvKeyRow), mut)
			if err != nil {
				dvid.Errorf("Failed to delete row")
			}
		} else {

			emptyTkey := make([]byte, 0)
			_, versionToDelete, err := ctx.SplitKey(emptyTkey)
			if err != nil {
				dvid.Errorf("Error in DeleteAll(): %v\n", err)
				return false
			}

			for _, readItem := range r[familyName] {

				verKey, err := decodeKey(readItem.Column)
				if err != nil {
					dvid.Errorf("Error in DeleteAll(): %v\n", err)
					return false
				}

				if bytes.Equal(verKey, versionToDelete) {
					mut := api.NewMutation()
					mut.DeleteCellsInColumn(familyName, encodeKey(verKey))
					err := tbl.Apply(db.ctx, encodeKey(unvKeyRow), mut)
					if err != nil {
						dvid.Errorf("Failed to DeleteCellsInColumn in DeleteAll()")
					}
					return true // One I found the version I don't have to keep serching for it.
				}
			}
		}

		return true // keep going
	}, api.RowFilter(api.StripValueFilter()))

	return err
}

func (db *BigTable) Close() {
	if db == nil {
		dvid.Errorf("Can't call Close() on nil BigTable")
		return
	}

	if client != nil {
		client.Close()
	}
}

func (db *BigTable) Equal(c dvid.StoreConfig) bool {
    bt, err := parseConfig(c)
    if err != nil {
        dvid.Errorf("unable to compare store config (%v) to BigTable: %v\n", c, err)
        return false
    }
	if db.project == bt.project && db.zone == bt.zone && db.cluster == bt.cluster && db.table == bt.table {
		return true
	}
	return false
}

// Util
// sliceContains reports whether the provided string is present in the given slice of strings.
func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}

func encodeKey(k []byte) string {
	return base64.URLEncoding.EncodeToString(k)
}

func decodeKey(b64key string) ([]byte, error) {

	//Columns contains the family name as a prefix, ex: versions:AAAAAQAAAAA=
	//If ther eis no such a prefix the string is return unmodified
	b64key = strings.TrimPrefix(b64key, fmt.Sprintf("%s:", familyName))

	key, err := base64.URLEncoding.DecodeString(b64key)
	if err != nil {
		return nil, fmt.Errorf("Failed to decode string %s", b64key)
	}

	return key, nil
}

func getValue(r api.Row, verKey []byte) ([]byte, error) {

	for _, readItem := range r[familyName] {

		//readItem.Column contains the family prefix as part of the key,
		//We have to trim the prefix and decode to bytes.
		//If we try to compare this by encoding the verKey it wouldn't work
		itemVer, err := decodeKey(readItem.Column)
		if err != nil {
			return nil, fmt.Errorf("Error in getValue(): %s\n", err)
		}

		if bytes.Equal(itemVer, verKey) {
			return readItem.Value, nil
		}
	}

	//Debug
	dvid.Errorf("Row available %s", r[familyName])
	for _, readItem := range r[familyName] {

		itemVer, _ := decodeKey(readItem.Column)
		dvid.Errorf("Versions available %s", itemVer)

	}

	return nil, fmt.Errorf("Failed to find version %s in Row", verKey)
}

func removeType(key storage.TKey) storage.Key {

	return storage.Key(key)
}

// --- Batcher interface ----

type goBatch struct {
	db  *BigTable
	ctx storage.Context
	kvs []storage.TKeyValue
}

// NewBatch returns an implementation that allows batch writes
func (db *BigTable) NewBatch(ctx storage.Context) storage.Batch {
	if ctx == nil {
		dvid.Criticalf("Received nil context in NewBatch()")
		return nil
	}
	return &goBatch{db, ctx, []storage.TKeyValue{}}
}

// --- Batch interface ---

func (batch *goBatch) Delete(tkey storage.TKey) {

	batch.db.Delete(batch.ctx, tkey)
}

func (batch *goBatch) Put(tkey storage.TKey, value []byte) {
	if batch == nil || batch.ctx == nil {
		dvid.Criticalf("Received nil batch or nil batch context in batch.Put()\n")
		return
	}

	batch.db.Put(batch.ctx, tkey, value)

	batch.kvs = append(batch.kvs, storage.TKeyValue{tkey, value})
	storage.StoreKeyBytesWritten <- len(tkey)
	storage.StoreValueBytesWritten <- len(value)
}

func (batch *goBatch) Commit() error {

	return batch.db.PutRange(batch.ctx, batch.kvs)

}
