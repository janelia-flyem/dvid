// +build badger

package tarsupervoxels

import "testing"

func TestBadgerTarballRoundTrip(t *testing.T) {
	testTarball(t, "badger")
}
