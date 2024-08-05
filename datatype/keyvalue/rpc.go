package keyvalue

import (
	"context"
	"fmt"
	"sync"

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
	var uuidStr, dataName, cmdStr, ref, workersStr string
	cmd.CommandArgs(1, &uuidStr, &dataName, &cmdStr, &ref, &workersStr)

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
	bucket, err := storage.OpenBucket(ref)
	if err != nil {
		return err
	}

	go d.dumpCloudInBackground(bucket, versionID, workers)

	reply.Output = []byte(fmt.Sprintf("Dumping all key-values to cloud %q for keyvalue %q, uuid %s\n",
		ref, d.DataName(), uuidStr))
	return nil
}

func (d *Data) dumpCloudInBackground(bucket *blob.Bucket, versionID dvid.VersionID, workers int) {
	defer bucket.Close()

	// Start goroutines to write key-value pairs to cloud storage.
	wg := new(sync.WaitGroup)
	ch := make(chan *storage.TKeyValue, 1000)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go d.writeWorker(i+1, wg, bucket, ch)
	}

	// Send all key-values to the writer goroutines.
	numKV, err := d.sendKVsToWriters(versionID, ch)
	close(ch)
	wg.Wait()
	if err != nil {
		dvid.Errorf("Background dump-cloud had error with %d key-values transferred: %v\n", numKV, err)
	} else {
		dvid.Infof("Background dump-cloud completed: %d key-values transferred\n", numKV)
	}
}

// writeWorker writes key-value pairs to cloud storage.
func (d *Data) writeWorker(workerNum int, wg *sync.WaitGroup, bucket *blob.Bucket, ch chan *storage.TKeyValue) {
	ctx := context.Background()
	written := 0
	for kv := range ch {
		key, err := DecodeTKey(kv.K)
		if err != nil {
			dvid.Errorf("Error decoding TKey: %v\n", err)
			continue
		}
		value, _, err := dvid.DeserializeData(kv.V, true)
		if err != nil {
			dvid.Errorf("unable to deserialize data for key %q\n", string(key))
			continue
		}
		opts := blob.WriterOptions{
			BufferSize: len(value),
			//ContentType: "text/plain",
		}
		if err := bucket.WriteAll(ctx, key, value, &opts); err != nil {
			dvid.Errorf("Error writing key %q (%d bytes) to cloud bucket: %v\n", string(key), len(kv.V), err)
			continue
		}
		written++
		if written % 1000 == 0 {
			dvid.Infof("Worker %d has written %d kv\n", workerNum, written)
		}
	}
	dvid.Infof("Worker %d wrote %d kv\n", workerNum, written)
	wg.Done()
}

// iterate through all key-value pairs and send them to the writer channel.
func (d *Data) sendKVsToWriters(versionID dvid.VersionID, ch chan *storage.TKeyValue) (numKV uint64, err error) {
	ctx := datastore.NewVersionedCtx(d, versionID)

	var db storage.OrderedKeyValueDB
	if db, err = datastore.GetOrderedKeyValueDB(d); err != nil {
		return
	}
	first := storage.MinTKey(keyStandard)
	last := storage.MaxTKey(keyStandard)
	err =  db.ProcessRange(ctx, first, last, &storage.ChunkOp{}, func(c *storage.Chunk) error {
		if c == nil || c.TKeyValue == nil || c.TKeyValue.V == nil {
			return nil
		}

		ch <- c.TKeyValue
		numKV++
		if numKV % 1000 == 0 {
			dvid.Infof("Sent %d kv pairs from instance %q for cloud storage.\n", numKV, d.DataName())
		}
		return nil
	})
	dvid.Infof("Sent total of %d kv pairs from instance %q for cloud storage.\n", numKV, d.DataName())
	return
}
