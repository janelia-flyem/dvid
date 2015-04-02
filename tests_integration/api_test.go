package tests_integration

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/server"
	"github.com/janelia-flyem/dvid/tests"

	// Declare the data types the DVID server should support
	_ "github.com/janelia-flyem/dvid/datatype/keyvalue"
	_ "github.com/janelia-flyem/dvid/datatype/labelblk"
	_ "github.com/janelia-flyem/dvid/datatype/roi"
)

type labelVol struct {
	size      dvid.Point3d
	blockSize dvid.Point3d
	offset    dvid.Point3d
}

// Each voxel in volume has sequential labels in X, Y, then Z order.
// volSize = size of volume in blocks
// blockSize = size of a block in voxels
func (vol labelVol) postLabelVolume(t *testing.T, labelsName string, uuid dvid.UUID) {
	server.CreateTestInstance(t, uuid, "labelblk", labelsName, dvid.Config{})

	offset := vol.offset

	nx := vol.size[0] * vol.blockSize[0]
	ny := vol.size[1] * vol.blockSize[1]
	nz := vol.size[2] * vol.blockSize[2]

	buf := make([]byte, nx*ny*nz*8)
	var label uint64
	var x, y, z, v int32
	for z = 0; z < nz; z++ {
		for y = 0; y < ny; y++ {
			for x = 0; x < nx; x++ {
				label++
				binary.LittleEndian.PutUint64(buf[v:v+8], label)
				v += 8
			}
		}
	}
	apiStr := fmt.Sprintf("%snode/%s/%s/raw/0_1_2/%d_%d_%d/%d_%d_%d", server.WebAPIPath,
		uuid, labelsName, nx, ny, nz, offset[0], offset[1], offset[2])
	server.TestHTTP(t, "POST", apiStr, bytes.NewBuffer(buf))
}

// the label in the test volume should just be the voxel index + 1 when iterating in ZYX order.
// The passed (x,y,z) should be world coordinates, not relative to the volume offset.
func (vol labelVol) label(x, y, z int32) uint64 {
	if x < vol.offset[0] || x >= vol.offset[0]+vol.size[0]*vol.blockSize[0] {
		return 0
	}
	if y < vol.offset[1] || y >= vol.offset[1]+vol.size[1]*vol.blockSize[1] {
		return 0
	}
	if z < vol.offset[2] || z >= vol.offset[2]+vol.size[2]*vol.blockSize[2] {
		return 0
	}
	x -= vol.offset[0]
	y -= vol.offset[1]
	z -= vol.offset[2]
	nx := vol.size[0] * vol.blockSize[0]
	nxy := nx * vol.size[1] * vol.blockSize[1]
	return uint64(z*nxy) + uint64(y*nx) + uint64(x+1)
}

type sliceTester struct {
	orient string
	width  int32
	height int32
	offset dvid.Point3d // offset of slice
}

func (s sliceTester) apiStr(uuid dvid.UUID, name string) string {
	return fmt.Sprintf("%snode/%s/%s/raw/%s/%d_%d/%d_%d_%d", server.WebAPIPath,
		uuid, name, s.orient, s.width, s.height, s.offset[0], s.offset[1], s.offset[2])
}

// make sure the given labels match what would be expected from the test volume.
func (s sliceTester) testLabel(t *testing.T, vol labelVol, img *dvid.Image) {
	data := img.Data()
	var x, y, z int32
	i := 0
	switch s.orient {
	case "xy":
		for y = 0; y < s.height; y++ {
			for x = 0; x < s.width; x++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := x + s.offset[0]
				vy := y + s.offset[1]
				vz := s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	case "xz":
		for z = 0; z < s.height; z++ {
			for x = 0; x < s.width; x++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := x + s.offset[0]
				vy := s.offset[1]
				vz := z + s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	case "yz":
		for z = 0; z < s.height; z++ {
			for y = 0; x < s.width; x++ {
				label := binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
				i++
				vx := s.offset[0]
				vy := y * s.offset[1]
				vz := z + s.offset[2]
				expected := vol.label(vx, vy, vz)
				if label != expected {
					t.Errorf("Bad label @ (%d,%d,%d): expected %d, got %d\n", vx, vy, vz, expected, label)
					return
				}
			}
		}
		return
	default:
		t.Fatalf("Unknown slice orientation %q\n", s.orient)
	}
}

func TestLabelsSyncing(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	uuid := dvid.UUID(server.NewTestRepo(t))
	if len(uuid) < 5 {
		t.Fatalf("Bad root UUID for new repo: %s\n", uuid)
	}

	// Create a labelblk instance
	vol := labelVol{
		size:      dvid.Point3d{5, 5, 5}, // in blocks
		blockSize: dvid.Point3d{32, 32, 32},
		offset:    dvid.Point3d{32, 64, 96},
	}
	vol.postLabelVolume(t, "labels", uuid)

	// TODO -- Test syncing across labelblk, labelvol, labelsz.
}

func TestCommitAndBranch(t *testing.T) {
	tests.UseStore()
	defer tests.CloseStore()

	apiStr := fmt.Sprintf("%srepos", server.WebAPIPath)
	r := server.TestHTTP(t, "POST", apiStr, nil)
	var jsonResp map[string]interface{}

	if err := json.Unmarshal(r, &jsonResp); err != nil {
		t.Fatalf("Unable to unmarshal repo creation response: %s\n", string(r))
	}
	v, ok := jsonResp["root"]
	if !ok {
		t.Fatalf("No 'root' metadata returned: %s\n", string(r))
	}
	uuidStr, ok := v.(string)
	if !ok {
		t.Fatalf("Couldn't cast returned 'root' data (%v) into string.\n", v)
	}
	uuid := dvid.UUID(uuidStr)

	// Shouldn't be able to create branch on open node.
	branchReq := fmt.Sprintf("%snode/%s/branch", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", branchReq, nil)

	// Add a keyvalue instance.
	server.CreateTestInstance(t, uuid, "keyvalue", "mykv", dvid.Config{})

	// Commit it.
	payload := bytes.NewBufferString(`{"note": "This is my test commit", "log": ["line1", "line2", "some more stuff in a line"]}`)
	apiStr = fmt.Sprintf("%snode/%s/commit", server.WebAPIPath, uuid)
	server.TestHTTP(t, "POST", apiStr, payload)

	// Make sure committed nodes can only be read.
	// We shouldn't be able to write to keyvalue..
	keyReq := fmt.Sprintf("%snode/%s/mykv/key/foo", server.WebAPIPath, uuid)
	server.TestBadHTTP(t, "POST", keyReq, bytes.NewBufferString("some data"))

	// Should be able to create branch now that we've committed parent.
	respData := server.TestHTTP(t, "POST", branchReq, nil)
	resp := struct {
		Child dvid.UUID `json:"child"`
	}{}
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Errorf("Expected 'child' JSON response.  Got %s\n", string(respData))
	}

	// We should be able to write to that keyvalue now in the child.
	keyReq = fmt.Sprintf("%snode/%s/mykv/key/foo", server.WebAPIPath, resp.Child)
	server.TestHTTP(t, "POST", keyReq, bytes.NewBufferString("some data"))
}
