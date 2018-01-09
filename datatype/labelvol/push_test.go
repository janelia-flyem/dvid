package labelvol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/storage"
)

// test ROI has offset (32, 32, 32) and size (64, 64, 64)
var testSpans = []dvid.Span{
	dvid.Span{1, 1, 1, 2}, dvid.Span{1, 2, 1, 2},
	dvid.Span{2, 1, 1, 2}, dvid.Span{2, 2, 1, 2},
}

func getROIReader() io.Reader {
	jsonBytes, err := json.Marshal(testSpans)
	if err != nil {
		log.Fatalf("Can't encode spans into JSON: %v\n", err)
	}
	return bytes.NewReader(jsonBytes)
}

func TestFilter(t *testing.T) {
	if err := server.OpenTest(); err != nil {
		t.Fatalf("can't open test server: %v\n", err)
	}
	defer server.CloseTest()

	// Create testbed volume and data instances
	uuid, _ := initTestRepo()
	var config dvid.Config

	server.CreateTestInstance(t, uuid, "labelblk", "labels", config)
	d, err := datastore.NewData(uuid, labelvolT, "bodies", config)
	if err != nil {
		t.Fatalf("Unable to create labelvol instance: %s\n", err)
	}
	server.CreateTestSync(t, uuid, "labels", "bodies")
	server.CreateTestSync(t, uuid, "bodies", "labels")

	// Populate the labels, which should automatically populate the labelvol
	_ = createLabelTestVolume(t, uuid, "labels")

	if err := datastore.BlockOnUpdating(uuid, "bodies"); err != nil {
		t.Fatalf("Error blocking on sync of labels -> bodies: %v\n", err)
	}

	// Create a ROI that will be used for filter test.
	server.CreateTestInstance(t, uuid, "roi", "myroi", config)
	roiRequest := fmt.Sprintf("%snode/%s/myroi/roi", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", roiRequest, getROIReader())

	// Create the filter spec
	fs := storage.FilterSpec(fmt.Sprintf("roi:myroi,%s", uuid))
	var filter storage.Filter
	filterer, ok := d.(storage.Filterer)
	if !ok {
		t.Fatalf("labelvol instance does not implement storage.Filterer\n")
	}
	filter, err = filterer.NewFilter(fs)
	if err != nil {
		t.Fatalf("Can't create filter from spec %q: %v\n", fs, err)
	}
	if filter == nil {
		t.Fatalf("No filter could be created from spec %q\n", fs)
	}

	// Test the filter.
	tkv := storage.TKeyValue{K: NewTKey(23, dvid.ChunkPoint3d{0, 0, 0}.ToIZYXString())}
	skip, err := filter.Check(&tkv)
	if !skip {
		t.Errorf("Expected filter check 1 to skip, instead filter.Check() returned not skip")
	}
	tkv = storage.TKeyValue{K: NewTKey(23, dvid.ChunkPoint3d{1, 1, 1}.ToIZYXString())}
	skip, err = filter.Check(&tkv)
	if skip {
		t.Errorf("Expected filter check 2 to not skip!")
	}
	tkv = storage.TKeyValue{K: NewTKey(23, dvid.ChunkPoint3d{2, 1, 2}.ToIZYXString())}
	skip, err = filter.Check(&tkv)
	if skip {
		t.Errorf("Expected filter check 2 to not skip!")
	}
	tkv = storage.TKeyValue{K: NewTKey(23, dvid.ChunkPoint3d{3, 1, 1}.ToIZYXString())}
	skip, err = filter.Check(&tkv)
	if !skip {
		t.Errorf("Expected filter check 3 to skip!")
	}
}
