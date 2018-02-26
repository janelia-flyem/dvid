package labelmap

import (
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

// Stress-test indexing during ingestion handling.
func TestIndexing(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := datastore.NewTestRepo()
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}
	server.CreateTestInstance(t, uuid, "labelmap", "labels", dvid.Config{})
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
				ldm := make(map[uint64]blockDiffMap)
				izyxstr := dvid.ChunkPoint3d{bx, by, bz}.ToIZYXString()
				for i := uint64(0); i < 4; i++ {
					j := label + i*maxLabel
					bdm, found := ldm[j]
					if !found {
						bdm = make(blockDiffMap)
						ldm[j] = bdm
					}
					ldiff, found := bdm[izyxstr]
					if !found {
						ldiff = labelDiff{delta: make(map[uint64]int32), present: true}
					}
					ldiff.delta[j] = numVoxels
					bdm[izyxstr] = ldiff
				}
				for label, bdm := range ldm {
					ChangeLabelIndex(d, v, label, bdm)
				}
			}
		}
	}

	// make sure are indexing on disk is correct.
	for label = 1; label <= maxLabel*4; label++ {
		meta, err := GetLabelIndex(d, v, label)
		if err != nil {
			t.Fatalf("label %d had no meta data associated with it\n", label)
		}
		if len(meta.Blocks) != 5 {
			t.Errorf("label %d had %d blocks: %s\n", label, len(meta.Blocks), meta.Blocks)
		}
	}
}
