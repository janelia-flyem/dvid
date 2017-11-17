package labelarray

import (
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

// Stress-test indexing during ingestion handling.
func TestIndexing(t *testing.T) {
	datastore.OpenTest()
	defer datastore.CloseTest()

	uuid, _ := datastore.NewTestRepo()
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}
	server.CreateTestInstance(t, uuid, "labelarray", "labels", dvid.Config{})
	d, err := GetByUUIDName(uuid, "labels")
	if err != nil {
		t.Fatal(err)
	}
	v, err := datastore.VersionFromUUID(uuid)
	if err != nil {
		t.Fatal(err)
	}

	// pretend we are ingesting bvsize x bvsize x bvsize blocks and for labels are tiled in 5x5x5
	// block subvolumes.
	var bvsize, bx, by, bz, numVoxels int32
	bvsize = 50
	numVoxels = 64 * 64 * 64 / 20
	maxLabel := uint64(bvsize*bvsize*bvsize) / 5
	var label uint64
	for bz = 0; bz < bvsize; bz++ {
		for by = 0; by < bvsize; by++ {
			for bx = 0; bx < bvsize; bx++ {
				if bx%5 == 0 {
					label++
				}
				ldm := make(labelDiffMap)
				izyxstr := dvid.ChunkPoint3d{bx, by, bz}.ToIZYXString()
				for i := uint64(0); i < 4; i++ {
					j := label + i*maxLabel
					bdm, found := ldm[j]
					if !found {
						bdm = make(blockDiffMap)
					}
					bdm[izyxstr] = labelDiff{numVoxels, true}
					ldm[j] = bdm
				}
				for label, bdm := range ldm {
					change := labelChange{v, label, bdm}
					shard := label % numLabelHandlers
					d.indexCh[shard] <- change
				}
			}
		}
	}

	// wait
	for {
		var maxShard, maxQueued int
		for i := 0; i < numLabelHandlers; i++ {
			queued := len(d.indexCh[i])
			if queued > maxQueued {
				maxShard = i
				maxQueued = queued
			}
		}
		if maxQueued > 0 {
			dvid.Infof("Waiting.  Shard %d had queue %d\n", maxShard, maxQueued)
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	time.Sleep(1 * time.Second)

	// make sure are indexing on disk is correct.
	ctx := datastore.NewVersionedCtx(d, v)
	for label = 1; label <= maxLabel*4; label++ {
		meta, err := d.getLabelMeta(ctx, labels.NewSet(label), 0, dvid.Bounds{})
		if err != nil {
			t.Fatalf("label %d had no meta data associated with it\n", label)
		}
		if len(meta.Blocks) != 5 {
			t.Errorf("label %d had %d blocks: %s\n", label, len(meta.Blocks), meta.Blocks)
		}
	}
}
