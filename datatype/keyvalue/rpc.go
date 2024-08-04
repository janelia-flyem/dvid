package keyvalue

import (
	"context"
	"fmt"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

// put handles a PUT command-line request.
func (d *Data) put(cmd datastore.Request, reply *datastore.Response) error {
	if len(cmd.Command) < 5 {
		return fmt.Errorf("the key name must be specified after 'put'")
	}
	if len(cmd.Input) == 0 {
		return fmt.Errorf("no data was passed into standard input")
	}
	var uuidStr, dataName, cmdStr, keyStr string
	cmd.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &keyStr)

	_, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	// Store data
	if !d.Versioned() {
		// Map everything to root version.
		versionID, err = datastore.GetRepoRootVersion(versionID)
		if err != nil {
			return err
		}
	}
	ctx := datastore.NewVersionedCtx(d, versionID)
	if err = d.PutData(ctx, keyStr, cmd.Input); err != nil {
		return fmt.Errorf("error on put to key %q for keyvalue %q: %v", keyStr, d.DataName(), err)
	}

	reply.Output = []byte(fmt.Sprintf("Put %d bytes into key %q for keyvalue %q, uuid %s\n",
		len(cmd.Input), keyStr, d.DataName(), uuidStr))
	return nil
}

// dumpCloud stores all key-value pairs in cloud storage.
func (d *Data) dumpCloud(cmd datastore.Request, reply *datastore.Response) error {
	if len(cmd.Command) < 5 {
		return fmt.Errorf("dump-cloud must be followed by <cloud path> <optional # workers>")
	}
	var uuidStr, dataName, cmdStr, refStr, workersStr string
	cmd.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &refStr, &workersStr)

	var workers int = 20
	if workersStr != "" {
		if _, err := fmt.Sscanf(workersStr, "%d", &workers); err != nil {
			return fmt.Errorf("bad number of workers (%q): %v", workersStr, err)
		}
	}

	_, versionID, err := datastore.MatchingUUID(uuidStr)
	if err != nil {
		return err
	}

	if !d.Versioned() {
		// Map everything to root version.
		versionID, err = datastore.GetRepoRootVersion(versionID)
		if err != nil {
			return err
		}
	}

	bucket, err := storage.OpenBucket(refStr)
	if err != nil {
		return err
	}
	defer bucket.Close()

	// Start goroutines to write key-value pairs to cloud storage.
	ch := make(chan *storage.TKeyValue, 1000)
	for i := 0; i < workers; i++ {
		go d.writeWorker(bucket, ch)
	}

	// Send all key-values to the writer goroutines.
	d.sendKVsToWriters(versionID, ch)

	reply.Output = []byte(fmt.Sprintf("Dumped all key-values to cloud %q for keyvalue %q, uuid %s\n",
		refStr, d.DataName(), uuidStr))
	return nil
}

// writeWorker writes key-value pairs to cloud storage.
func (d *Data) writeWorker(bucket *blob.Bucket, ch chan *storage.TKeyValue) {
	for kv := range ch {
		key, err := DecodeTKey(kv.K)
		if err != nil {
			dvid.Errorf("Error decoding TKey: %v\n", err)
			continue
		}
		w, err := bucket.NewWriter(context.Background(), key, nil)
		if err != nil {
			dvid.Errorf("Error creating writer for key %q: %v\n", key, err)
			continue
		}
		if _, err := w.Write(kv.V); err != nil {
			dvid.Errorf("Error writing key %q to cloud bucket: %v\n", key, err)
			continue
		}
		if err := w.Close(); err != nil {
			dvid.Errorf("Error closing after key %q to cloud bucket: %v\n", key, err)
		}
	}
}

// iterate through all key-value pairs and send them to the writer channel.
func (d *Data) sendKVsToWriters(versionID dvid.VersionID, ch chan *storage.TKeyValue) error {
	ctx := datastore.NewVersionedCtx(d, versionID)
	db, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return err
	}
	first := storage.MinTKey(keyStandard)
	last := storage.MaxTKey(keyStandard)
	return db.ProcessRange(ctx, first, last, &storage.ChunkOp{}, func(c *storage.Chunk) error {
		if c == nil || c.TKeyValue == nil || c.TKeyValue.V == nil {
			return nil
		}
		ch <- c.TKeyValue
		return nil
	})
}
