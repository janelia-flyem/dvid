package tarsupervoxels

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

var (
	kvtype datastore.TypeService
	testMu sync.Mutex
)

// Sets package-level testRepo and TestVersionID
func initTestRepo() (dvid.UUID, dvid.VersionID) {
	testMu.Lock()
	defer testMu.Unlock()
	if kvtype == nil {
		var err error
		kvtype, err = datastore.TypeServiceByName(TypeName)
		if err != nil {
			log.Fatalf("Can't get tarsupervoxels type: %s\n", err)
		}
	}
	return datastore.NewTestRepo()
}

func testTarball(t *testing.T, storetype storage.Alias) {
	testConfig := server.TestConfig{
		KVStoresMap: storage.DataMap{"tarsupervoxels": storetype},
	}
	if err := server.OpenTest(testConfig); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	uuid, _ := initTestRepo()
	var config dvid.Config
	labelname := "labels"
	server.CreateTestInstance(t, uuid, "labelmap", labelname, config)
	tarsvname := "blobs"
	config.Set("Extension", "dat")
	server.CreateTestInstance(t, uuid, "tarsupervoxels", tarsvname, config)
	server.CreateTestSync(t, uuid, tarsvname, labelname)

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}

	// Put in supervoxels 1-64 at intersection of 8 default blocks.
	n := 64
	voxels := make([]byte, n*n*n*8)
	for i := 0; i < n*n*n; i++ {
		binary.LittleEndian.PutUint64(voxels[i*8:i*8+8], uint64((i%64)+1))
	}
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/64_64_64", server.WebAPIPath,
		uuid, labelname, n, n, n)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(voxels))
	if err := datastore.BlockOnUpdating(uuid, dvid.InstanceName(labelname)); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}

	// Merge a few supervoxels to create bodies to test.
	apiStr = fmt.Sprintf("%snode/%s/labels/merge", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString("[30, 10, 15, 18, 19, 20, 21, 64]"))

	// Add tarball data for first 63 supervoxels.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for i := uint64(1); i <= 63; i++ {
		data := fmt.Sprintf("This is the data for supervoxel %d.", i)
		hdr := &tar.Header{
			Name: fmt.Sprintf("%d.dat", i),
			Mode: 0755,
			Size: int64(len(data)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("unable to write tar file header for supervoxel %d: %v\n", i, err)
		}
		if _, err := tw.Write([]byte(data)); err != nil {
			t.Fatalf("unable to write data for sueprvoxel %d: %v\n", i, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("bad tar file close: %v\n", err)
	}
	apiStr = fmt.Sprintf("%snode/%s/%s/load", server.WebAPIPath, uuid, tarsvname)
	server.TestHTTP(t, "POST", apiStr, &buf)

	// Test single POST for last supervoxel
	apiStr = fmt.Sprintf("%snode/%s/%s/supervoxel/64", server.WebAPIPath, uuid, tarsvname)
	server.TestHTTP(t, "POST", apiStr, bytes.NewBufferString("This is the data for supervoxel 64."))

	// Check existence through HEAD
	apiStr = fmt.Sprintf("%snode/%s/%s/tarfile/80", server.WebAPIPath, uuid, tarsvname)
	server.TestBadHTTP(t, "HEAD", apiStr, nil)

	apiStr = fmt.Sprintf("%snode/%s/%s/tarfile/10", server.WebAPIPath, uuid, tarsvname) // was merged
	server.TestBadHTTP(t, "HEAD", apiStr, nil)

	apiStr = fmt.Sprintf("%snode/%s/%s/tarfile/60", server.WebAPIPath, uuid, tarsvname)
	server.TestHTTP(t, "HEAD", apiStr, nil)

	// Check existence through /exists endpoint
	apiStr = fmt.Sprintf("%snode/%s/%s/exists", server.WebAPIPath, uuid, tarsvname)
	r := server.TestHTTP(t, "GET", apiStr, bytes.NewBufferString("[81, 10, 60]"))
	var existences []bool
	if err := json.Unmarshal(r, &existences); err != nil {
		t.Fatalf("error trying to unmarshal existence list: %v\n", err)
	}
	expectedList := []bool{false, true, true}
	if len(existences) != 3 {
		t.Fatalf("expected 3 existences returned, got %d: %v\n", len(existences), existences)
	}
	for i, existence := range existences {
		if expectedList[i] != existence {
			t.Fatalf("expected existence %d to be %t, got %t\n", i, expectedList[i], existence)
		}
	}

	// Get tarball for body.
	expected := labels.NewSet(30, 10, 15, 18, 19, 20, 21, 64)
	apiStr = fmt.Sprintf("%snode/%s/%s/tarfile/30", server.WebAPIPath, uuid, tarsvname)
	data := server.TestHTTP(t, "GET", apiStr, nil)

	buf2 := bytes.NewBuffer(data)
	tr := tar.NewReader(buf2)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("error parsing tar: %v\n", err)
		}
		var svdata bytes.Buffer
		if _, err := io.Copy(&svdata, tr); err != nil {
			t.Fatalf("error reading tar data: %v\n", err)
		}
		var supervoxel uint64
		var ext string
		if _, err := fmt.Sscanf(hdr.Name, "%d.%s", &supervoxel, &ext); err != nil {
			t.Fatalf("can't parse tar file name %q: %v\n", hdr.Name, err)
		}
		if ext != "dat" {
			t.Fatalf("bad extension for tar file name %q\n", hdr.Name)
		}
		if _, found := expected[supervoxel]; !found {
			t.Fatalf("got back supervoxel %d in tarfile, which is not in set %s\n", supervoxel, expected)
		}
		got := string(svdata.Bytes())
		if got != fmt.Sprintf("This is the data for supervoxel %d.", supervoxel) {
			t.Fatalf(`expected "This is the data for supervoxel %d.", got %q`, supervoxel, got)
		}
	}

	// Test single GET.
	apiStr = fmt.Sprintf("%snode/%s/%s/supervoxel/17", server.WebAPIPath, uuid, tarsvname)
	data = server.TestHTTP(t, "GET", apiStr, nil)
	if string(data) != "This is the data for supervoxel 17." {
		t.Fatalf("got bad supervoxel 17 data: %s\n", string(data))
	}
}

func TestTarballRoundTrip(t *testing.T) {
	testTarball(t, "filestore")
	testTarball(t, "basholeveldb")
}
