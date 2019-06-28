// +build basholeveldb

package tarsupervoxels

import "testing"

func TestBasholeveldbTarballRoundTrip(t *testing.T) {
	testTarball(t, "basholeveldb")
}
