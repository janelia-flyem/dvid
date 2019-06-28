// +build gbucket

package gbucket

import (
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

func TestGBucketMetadata(t *testing.T) {
	eng := storage.GetEngine("gbucket")
	if eng == nil {
		t.Fatalf("Init does not register 'gbucket' engine.\n")
	}
	// Make an empty GBucket just to check interfaces.
	gbucket := new(GBucket)
	var store dvid.Store = gbucket
	kvgetter, ok := store.(storage.KeyValueGetter)
	if !ok {
		t.Fatalf("GBucket should implement storage.KeyValueGetter interface but doesn't\n")
	}
	kvsetter, ok := kvgetter.(storage.KeyValueSetter)
	if !ok {
		t.Fatalf("GBucket should implement storage.KeyValueSetter interface but doesn't\n")
	}
	kvdb, ok := kvsetter.(storage.KeyValueDB)
	if !ok {
		t.Fatalf("GBucket should implement storage.KeyValueDB interface but doesn't\n")
	}
	if _, ok = kvdb.(storage.OrderedKeyValueDB); !ok {
		t.Fatalf("GBucket should implement storage.OrderedKeyValueDB interface but doesn't\n")
	}
}
