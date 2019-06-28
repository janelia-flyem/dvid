// +build filestore

package tarsupervoxels

import "testing"

func TestFilestoreTarballRoundTrip(t *testing.T) {
	testTarball(t, "filestore")
}
