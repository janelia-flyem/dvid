// +build swift

package swift

import (
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

func TestSwiftMetadata(t *testing.T) {
	eng := storage.GetEngine("swift")
	if eng == nil {
		t.Fatalf("Init does not register 'swift' engine.\n")
	}
	// Make an empty Swift just to check interfaces.
	swiftStore := new(Store)
	var store dvid.Store = swiftStore
	kvgetter, ok := store.(storage.KeyValueGetter)
	if !ok {
		t.Fatalf("Swift should implement storage.KeyValueGetter interface but doesn't\n")
	}
	kvsetter, ok := kvgetter.(storage.KeyValueSetter)
	if !ok {
		t.Fatalf("Swift should implement storage.KeyValueSetter interface but doesn't\n")
	}
	kvdb, ok := kvsetter.(storage.KeyValueDB)
	if !ok {
		t.Fatalf("Swift should implement storage.KeyValueDB interface but doesn't\n")
	}
	if _, ok = kvdb.(storage.OrderedKeyValueDB); !ok {
		t.Fatalf("Swift should implement storage.OrderedKeyValueDB interface but doesn't\n")
	}
}
