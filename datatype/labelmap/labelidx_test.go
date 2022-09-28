package labelmap

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	pb "google.golang.org/protobuf/proto"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
)

func checkGetIndices(t *testing.T, uuid dvid.UUID, indices ...*labels.Index) {
	labelList := make([]uint64, len(indices))
	for i, index := range indices {
		labelList[i] = index.Label
	}
	jsonBytes, err := json.Marshal(labelList)
	if err != nil {
		t.Fatalf("couldn't marshal labels: %v\n", err)
	}
	url := fmt.Sprintf("http://%snode/%s/labels/indices", server.WebAPIPath, uuid)
	data := server.TestHTTP(t, "GET", url, bytes.NewBuffer(jsonBytes))
	if len(data) == 0 {
		t.Fatalf("Read indices returned no bytes\n")
	}
	indicesRet := new(proto.LabelIndices)
	if err := pb.Unmarshal(data, indicesRet); err != nil {
		t.Fatalf("couldn't unmarshal indices: %v\n", err)
	}
	if len(indicesRet.Indices) != 3 {
		t.Fatalf("expected 3 returned label indices, got %d\n", len(indicesRet.Indices))
	}
	for i, index := range indices {
		indexRet := labels.Index{*(indicesRet.Indices[i])}
		if !index.Equal(indexRet) {
			t.Fatalf("expected index %d to be\n%v\n  but GOT:\n%v\n", i, index, indexRet)
		}
	}
}

func TestIngest(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	root, _ := initTestRepo()
	var config dvid.Config
	config.Set("MaxDownresLevel", "2")
	config.Set("BlockSize", "32,32,32") // Previous test data was on 32^3 blocks
	server.CreateTestInstance(t, root, "labelmap", "labels", config)

	// Post supervoxel volume
	original := createLabelTestVolume(t, root, "labels")
	if err := datastore.BlockOnUpdating(root, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}

	for label := uint64(1); label <= 4; label++ {
		indexURL := fmt.Sprintf("%snode/%s/labels/index/%d", server.WebAPIPath, root, label)
		serialization := server.TestHTTP(t, "GET", indexURL, nil)
		idx := new(labels.Index)
		if err := pb.Unmarshal(serialization, idx); err != nil {
			t.Fatalf("Unable to GET index/%d: %v\n", label, err)
		}
		if idx.Label != label {
			t.Fatalf("Expected index for label %d, got label %d index\n", label, idx.Label)
		}
		supervoxels := idx.GetSupervoxels()
		if len(supervoxels) != 1 {
			t.Fatalf("Expected index for label %d to have 1 supervoxel, got %d\n", label, len(supervoxels))
		}
		_, ok := supervoxels[label]
		if !ok {
			t.Errorf("Expeced index for label %d to have supervoxel %d, but wasn't present", label, label)
		}

		indexURL += "?metadata-only=true"
		respData := server.TestHTTP(t, "GET", indexURL, nil)
		respJSON := struct {
			NumVoxels   uint64 `json:"num_voxels"`
			LastMutID   uint64 `json:"last_mutid"`
			LastModTime string `json:"last_mod_time"`
			LastModUser string `json:"last_mod_user"`
			LastModApp  string `json:"last_mod_app"`
		}{}
		if err := json.Unmarshal(respData, &respJSON); err != nil {
			t.Errorf("Expected JSON response.  Got %s\n", string(respData))
		}
		if respJSON.NumVoxels != idx.NumVoxels() {
			t.Errorf("Expected num voxels %d, got %d\n", idx.NumVoxels(), respJSON.NumVoxels)
		}
	}

	checkReq := fmt.Sprintf("%snode/%s/labels/maxlabel", server.WebAPIPath, root)
	respData := server.TestHTTP(t, "GET", checkReq, nil)
	respMax := struct {
		MaxLabel uint64 `json:"maxlabel"`
	}{}
	if err := json.Unmarshal(respData, &respMax); err != nil {
		t.Errorf("Expected 'maxlabel' JSON response.  Got %s\n", string(respData))
	}
	if respMax.MaxLabel != 4 {
		t.Errorf("Expected maxlabel 4, got %d\n", respMax.MaxLabel)
	}

	checkReq = fmt.Sprintf("%snode/%s/labels/nextlabel", server.WebAPIPath, root)
	respData = server.TestHTTP(t, "GET", checkReq, nil)
	respNext := struct {
		NextLabel uint64 `json:"nextlabel"`
	}{}
	if err := json.Unmarshal(respData, &respNext); err != nil {
		t.Errorf("Expected 'nextlabel' JSON response.  Got %s\n", string(respData))
	}
	if respNext.NextLabel != 5 {
		t.Errorf("Expected nextlabel 5, got %d\n", respNext.NextLabel)
	}

	// commit and create child version
	payload := bytes.NewBufferString(`{"note": "Base Supervoxels"}`)
	commitReq := fmt.Sprintf("%snode/%s/commit", server.WebAPIPath, root)
	server.TestHTTP(t, "POST", commitReq, payload)

	newVersionReq := fmt.Sprintf("%snode/%s/newversion", server.WebAPIPath, root)
	respData = server.TestHTTP(t, "POST", newVersionReq, nil)
	respChild := struct {
		Child string `json:"child"`
	}{}
	if err := json.Unmarshal(respData, &respChild); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	child1 := dvid.UUID(respChild.Child)

	// Test labels in child shouldn't have changed.
	retrieved := newTestVolume(128, 128, 128)
	retrieved.get(t, child1, "labels", false)
	if err := retrieved.equals(original); err != nil {
		t.Errorf("before mapping: %v\n", err)
	}

	// POST new mappings and corresponding label indices
	var m proto.MappingOps
	m.Mappings = make([]*proto.MappingOp, 2)
	m.Mappings[0] = &proto.MappingOp{
		Mutid:    1,
		Mapped:   7,
		Original: []uint64{1, 2},
	}
	m.Mappings[1] = &proto.MappingOp{
		Mutid:    2,
		Mapped:   8,
		Original: []uint64{3},
	}
	serialization, err := pb.Marshal(&m)
	if err != nil {
		t.Fatal(err)
	}
	mappingReq := fmt.Sprintf("%snode/%s/labels/mappings", server.WebAPIPath, child1)
	server.TestHTTP(t, "POST", mappingReq, bytes.NewBuffer(serialization))

	mappingData := server.TestHTTP(t, "GET", mappingReq, nil)
	lines := strings.Split(strings.TrimSpace(string(mappingData)), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 lines for mapping, got %d lines\n", len(lines))
	} else {
		expected := map[uint64]uint64{1: 7, 2: 7, 3: 8}
		for i, line := range lines {
			var supervoxel, label uint64
			fmt.Sscanf(line, "%d %d", &supervoxel, &label)
			expectedLabel, found := expected[supervoxel]
			if !found {
				t.Errorf("got unknown mapping in line %d: %d -> %d\n", i, supervoxel, label)
			} else if expectedLabel != label {
				t.Errorf("expected supervoxel %d -> label %d, got %d\n", supervoxel, expectedLabel, label)
			}
		}
	}

	// Same check as above, but with binary format
	binaryMappingReq := fmt.Sprintf("%snode/%s/labels/mappings?format=binary", server.WebAPIPath, child1)
	server.TestHTTP(t, "POST", binaryMappingReq, bytes.NewBuffer(serialization))

	binaryMappingData := server.TestHTTP(t, "GET", binaryMappingReq, nil)
	if len(binaryMappingData) != 3*8*2 {
		t.Errorf("expected 3 pairs of uint64 but got %d bytes \n", len(binaryMappingData))
	} else {
		expected := map[uint64]uint64{1: 7, 2: 7, 3: 8}
		r := bytes.NewReader(binaryMappingData)
		for i := 0; i < 3; i++ {
			var supervoxel, label uint64
			binary.Read(r, binary.LittleEndian, &supervoxel)
			binary.Read(r, binary.LittleEndian, &label)
			expectedLabel, found := expected[supervoxel]
			if !found {
				t.Errorf("got unknown mapping in pair %d: %d -> %d\n", i, supervoxel, label)
			} else if expectedLabel != label {
				t.Errorf("expected supervoxel %d -> label %d, got %d\n", supervoxel, expectedLabel, label)
			}
		}
	}

	idx1 := body1.getIndex(t)
	idx2 := body2.getIndex(t)
	idx3 := body3.getIndex(t)
	checkGetIndices(t, child1, idx1, idx2, idx3)

	if err := idx1.Add(idx2); err != nil {
		t.Fatal(err)
	}
	idx1.Label = 7
	idx3.Label = 8

	ingestIndex(t, child1, idx1)
	ingestIndex(t, child1, idx3)

	checkReq = fmt.Sprintf("%snode/%s/labels/maxlabel", server.WebAPIPath, child1)
	respData = server.TestHTTP(t, "GET", checkReq, nil)
	if err := json.Unmarshal(respData, &respMax); err != nil {
		t.Errorf("Expected 'maxlabel' JSON response.  Got %s\n", string(respData))
	}
	if respMax.MaxLabel != 8 {
		t.Errorf("Expected maxlabel 8, got %d\n", respMax.MaxLabel)
	}

	checkReq = fmt.Sprintf("%snode/%s/labels/nextlabel", server.WebAPIPath, child1)
	respData = server.TestHTTP(t, "GET", checkReq, nil)
	if err := json.Unmarshal(respData, &respNext); err != nil {
		t.Errorf("Expected 'nextlabel' JSON response.  Got %s\n", string(respData))
	}
	if respNext.NextLabel != 9 {
		t.Errorf("Expected nextlabel 9, got %d\n", respNext.NextLabel)
	}

	blankIdx := new(labels.Index)
	blankIdx.Label = 2
	ingestIndex(t, child1, blankIdx)
	blankIdx.Label = 3
	ingestIndex(t, child1, blankIdx)

	checkNoSparsevol(t, child1, 2)
	checkNoSparsevol(t, child1, 3)

	// Test result
	retrieved.get(t, child1, "labels", false)
	if err := retrieved.equals(original); err == nil {
		t.Errorf("expected retrieved labels != original but they are identical after mapping\n")
	}
	bodyMerged := body1.add(body2)
	bodyMerged.checkSparsevolAPIs(t, child1, 7)
	body3.checkSparsevolAPIs(t, child1, 8)
	body4.checkSparsevolAPIs(t, child1, 4)

	// Commit and create new version
	payload = bytes.NewBufferString(`{"note": "First agglo"}`)
	commitReq = fmt.Sprintf("%snode/%s/commit", server.WebAPIPath, child1)
	server.TestHTTP(t, "POST", commitReq, payload)

	newVersionReq = fmt.Sprintf("%snode/%s/newversion", server.WebAPIPath, child1)
	respData = server.TestHTTP(t, "POST", newVersionReq, nil)
	if err := json.Unmarshal(respData, &respChild); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}
	child2 := dvid.UUID(respChild.Child)

	// POST second set of mappings to reset supervoxels to original and ingest label indices
	m.Mappings = make([]*proto.MappingOp, 3)
	m.Mappings[0] = &proto.MappingOp{
		Mutid:    3,
		Mapped:   1,
		Original: []uint64{1},
	}
	m.Mappings[1] = &proto.MappingOp{
		Mutid:    4,
		Mapped:   2,
		Original: []uint64{2},
	}
	m.Mappings[2] = &proto.MappingOp{
		Mutid:    5,
		Mapped:   3,
		Original: []uint64{3},
	}
	serialization, err = pb.Marshal(&m)
	if err != nil {
		t.Fatal(err)
	}
	mappingReq = fmt.Sprintf("%snode/%s/labels/mappings", server.WebAPIPath, child2)
	server.TestHTTP(t, "POST", mappingReq, bytes.NewBuffer(serialization))

	idx1 = body1.getIndex(t)
	idx2 = body2.getIndex(t)
	idx3 = body3.getIndex(t)

	ingestIndex(t, child2, idx1)
	ingestIndex(t, child2, idx2)
	ingestIndex(t, child2, idx3)
	blankIdx.Label = 7
	ingestIndex(t, child2, blankIdx)
	blankIdx.Label = 8
	ingestIndex(t, child2, blankIdx)

	// Test result
	checkSparsevolAPIs(t, child2)
	alternateUUID := child1[:8] + ":master"
	checkSparsevolAPIs(t, alternateUUID)
	checkNoSparsevol(t, child2, 7)
	checkNoSparsevol(t, child2, 8)

	retrieved.get(t, child2, "labels", false)
	if err := retrieved.equals(original); err != nil {
		t.Errorf("after remapping to original: %v\n", err)
	}
}

func writeTestBlock(t *testing.T, buf *bytes.Buffer, serialization []byte, blockCoord dvid.Point3d) {
	var gzipOut bytes.Buffer
	zw := gzip.NewWriter(&gzipOut)
	if _, err := zw.Write(serialization); err != nil {
		t.Fatal(err)
	}
	zw.Flush()
	zw.Close()
	gzipped := gzipOut.Bytes()
	writeTestInt32(t, buf, int32(len(gzipped)))
	n, err := buf.Write(gzipped)
	if err != nil {
		t.Fatalf("unable to write gzip block: %v\n", err)
	}
	if n != len(gzipped) {
		t.Fatalf("unable to write %d bytes to buffer, only wrote %d bytes\n", len(gzipped), n)
	}
}

func TestIngest2(t *testing.T) {
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
	if d == nil || v == 0 {
		t.Fatalf("bad version returned\n")
	}

	// Exercise POST /blocks using no-indexing.
	blockCoords := []dvid.Point3d{
		{1, 2, 3},
		{2, 2, 3},
		{1, 3, 4},
		{2, 3, 4},
		{3, 3, 4},
		{4, 3, 5},
	}
	var data [6]testData
	var buf bytes.Buffer
	for i, blockCoord := range blockCoords {
		writeTestInt32(t, &buf, blockCoord[0])
		writeTestInt32(t, &buf, blockCoord[1])
		writeTestInt32(t, &buf, blockCoord[2])
		if i < len(testFiles) {
			data[i] = loadTestData(t, testFiles[i])
		} else {
			var td testData
			td.u = make([]uint64, 64*64*64)
			for i := 0; i < 64*64*64; i++ {
				td.u[i] = 91748 * uint64(i)
			}
			td.b, err = labels.MakeBlock(dvid.AliasUint64ToByte(td.u), dvid.Point3d{64, 64, 64})
			if err != nil {
				t.Fatal(err)
			}
			data[i] = td
		}
		serialization, err := data[i].b.MarshalBinary()
		if err != nil {
			t.Fatalf("unable to MarshalBinary block: %v\n", err)
		}
		writeTestBlock(t, &buf, serialization, blockCoord)
	}

	apiStr := fmt.Sprintf("%snode/%s/labels/blocks?noindexing=true", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, &buf)

	if err := datastore.BlockOnUpdating(uuid, "labels"); err != nil {
		t.Fatalf("Error blocking on sync of labels: %v\n", err)
	}

	// Add malformed blocks
	blockCoords = append(blockCoords, dvid.Point3d{5, 3, 5})
	blockCoords = append(blockCoords, dvid.Point3d{6, 3, 5})
	buf.Reset()
	prevRecs := len(testFiles) + 2
	for i, blockCoord := range blockCoords {
		writeTestInt32(t, &buf, blockCoord[0])
		writeTestInt32(t, &buf, blockCoord[1])
		writeTestInt32(t, &buf, blockCoord[2])
		if i < prevRecs {
			serialization, err := data[i].b.MarshalBinary()
			if err != nil {
				t.Fatalf("unable to MarshalBinary block: %v\n", err)
			}
			writeTestBlock(t, &buf, serialization, blockCoord)
		} else {
			emptySlice := []byte{38, 247} // random bytes
			writeTestBlock(t, &buf, emptySlice, blockCoord)
		}
	}
	apiStr = fmt.Sprintf("%snode/%s/labels/blocks?noindexing=true", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", apiStr, &buf)

	buf.Reset()
	for i := int32(0); i < 3; i++ {
		bcoord := dvid.Point3d{i * 10, i * 11, i * 12}
		writeTestInt32(t, &buf, bcoord[0])
		writeTestInt32(t, &buf, bcoord[1])
		writeTestInt32(t, &buf, bcoord[2])
		writeTestBlock(t, &buf, []byte{}, bcoord)
	}
	apiStr = fmt.Sprintf("%snode/%s/labels/blocks?noindexing=true", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", apiStr, &buf)

	buf.Reset()
	for i := int32(0); i < 3; i++ {
		bcoord := dvid.Point3d{i * 10, i * 11, i * 12}
		writeTestInt32(t, &buf, bcoord[0])
		writeTestInt32(t, &buf, bcoord[1])
		writeTestInt32(t, &buf, bcoord[2])
		writeTestInt32(t, &buf, 0)
	}
	apiStr = fmt.Sprintf("%snode/%s/labels/blocks?noindexing=true", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", apiStr, &buf)
}
